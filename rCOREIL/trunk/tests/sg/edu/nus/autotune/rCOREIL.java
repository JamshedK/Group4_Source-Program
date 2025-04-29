//'Test 2 with written logs'
package sg.edu.nus.autotune;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.BitSet;

import org.jblas.DoubleMatrix;
import org.slf4j.Logger;

import edu.ucsc.dbtune.workload.FileWorkloadReader;
import edu.ucsc.dbtune.workload.SQLStatement;

import static org.slf4j.LoggerFactory.getLogger;
import static sg.edu.nus.autotune.Execution.DEFAULT_RESULT_FILE_PREFIX;

public class rCOREIL extends Execution {

    private static final String RESULT_FILE_PREFIX = null;
    private static final Logger LOG = getLogger("e2s2");
    private static final boolean DEBUG = true;
    private static final boolean EXPLOR = false;
    private static final int SEED = 1;

    private BitSet _explored = new BitSet();
    private PostgresDATA _data;

    public rCOREIL(DataConnectivity data) throws SQLException {
        super(data);
        this._data = (PostgresDATA) data;

        if (RESULT_FILE_PREFIX == null || RESULT_FILE_PREFIX.isEmpty()) {
            _resultFilePrefix = DEFAULT_RESULT_FILE_PREFIX;
        } else {
            _resultFilePrefix = RESULT_FILE_PREFIX;
        }
    }

    private AbsConf compute(AbsConf s0, String sql, BitSet bs1, BitSet bs2, DoubleMatrix eta, DoubleMatrix theta) throws SQLException {
        BitSet candAdd = _data.getAddCandidate(sql, s0);
        BitSet candDrop = s0.getDropCandidate(sql);

        DoubleMatrix eta_vector = AbsConf.toEtaVector(s0, bs1, bs2);
        DoubleMatrix s0_vector = s0.toVector();
        AbsConf s1 = s0.clone();

        DoubleMatrix zeta_v0 = AbsConf.toZetaVector(s0, s0);
        DoubleMatrix eta_v0 = eta_vector;
        DoubleMatrix theta_v0 = s0_vector;
        double minCost = eta.dot(zeta_v0) + eta.dot(eta_v0) + _gamma * theta.dot(theta_v0);

        for (int i = candAdd.previousSetBit(candAdd.length()); i >= 0; i = candAdd.previousSetBit(i - 1)) {
            if (s1.covers(i)) continue;

            AbsConf s = s0.clone();
            s.add(i);

            DoubleMatrix s_vector = s.toVector();
            DoubleMatrix zeta_v = AbsConf.toZetaVector(s0, s);
            DoubleMatrix eta_v = AbsConf.toEtaVector(s, bs1, bs2);
            DoubleMatrix theta_v = s_vector;

            double cost = eta.dot(zeta_v) + eta.dot(eta_v) + _gamma * theta.dot(theta_v);

            if (cost < minCost) {
                s1.add(i);
                _explored.or(_data.getCoveredBy(i));
            }
        }

        for (int i = candDrop.nextSetBit(0); i >= 0; i = candDrop.nextSetBit(i + 1)) {
            if (_data.isPrimaryKey(i)) continue;

            AbsConf s = s0.clone();
            s.drop(i);

            DoubleMatrix s_vector = s.toVector();
            DoubleMatrix eta_v = AbsConf.toEtaVector(s, bs1, bs2);
            DoubleMatrix theta_v = s_vector;

            double cost = 0 + eta.dot(eta_v) + _gamma * theta.dot(theta_v);

            if (cost < minCost) {
                _explored.set(i);
            }
        }

        return s1;
    }

    @Override
    protected int run() throws Exception {
        AbsConf s0 = _data.getCurrentConfigration();
        DoubleMatrix s0_vector = s0.toVector();

        ridge LS_eta = new ridge(_data.getNumOfIndexes() * 2 - 1);
        DoubleMatrix eta = LS_eta.getVector();

        LSTD LS_theta = new LSTD(_data.getNumOfIndexes(), _gamma, 100);
        DoubleMatrix theta = LS_theta.getVector();

        int index = 0;

        FileWorkloadReader wl = new FileWorkloadReader(_data.getWorkloadsFoldername() + _data.getWorkloadName());

        LOG.info("# new algo with " + _data.getWorkloadName() + " with " + _gamma);
        _output.write("# new algo with " + _data.getWorkloadName() + " with " + _gamma);
        _output.write("\n\n==== BEGIN WORKLOAD ====");

        for (SQLStatement sqls : wl) {
            String sql = sqls.getSQL();
            index++;

            _output.write("\n\n===== Query " + index + " =====");
            _output.write("\nSQL: " + sql);

            try {
                // Print EXPLAIN PLAN
                String explainSQL = "EXPLAIN (ANALYZE, FORMAT TEXT) " + sql;
                Connection conn = PostgresDATA.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(explainSQL);
                _output.write("\nEXPLAIN Plan:");
                while (rs.next()) {
                    _output.write("\n" + rs.getString(1));
                }
                stmt.close();

                double startTime = System.nanoTime();
                double start_alg_time = System.nanoTime();

                BitSet bs1 = _data.getAddCandidate(sql);
                BitSet bs2 = _data.getDropCandidate(sql);
                AbsConf s1 = compute(s0, sql, bs1, bs2, eta, theta);
                DoubleMatrix s1_vector = s1.toVector();

                DoubleMatrix zeta_vector = AbsConf.toZetaVector(s0, s1);
                double s0_change_cost = s0.whatif_changeToCost(s1);

                double zeta_error = eta.dot(zeta_vector) - s0_change_cost;
                eta = LS_eta.get(zeta_vector, zeta_error);

                DoubleMatrix eta_vector = AbsConf.toEtaVector(s1, bs1, bs2);
                double estimated_cost1 = eta.dot(zeta_vector);
                double estimated_cost2 = eta.dot(eta_vector);
                double s1_evaluate = s1.whatif_evaluate(sql);

                double eta_error = eta.dot(eta_vector) - s1_evaluate;
                eta = LS_eta.get(eta_vector, eta_error);

                double hat_theta = s0_change_cost + s1_evaluate;
                theta = LS_theta.get(s0_vector, s1_vector, hat_theta);

                double real_s1_evaluate = _data.execute(sql);

                s0_vector = s1_vector;
                s0 = s1.clone();

                double end_alg_time = System.nanoTime();
                double alg_time = (end_alg_time - start_alg_time) / 1_000_000.0;

                _output.write("\nOverhead Time (ms): " + alg_time);
                _output.write("\nCosts: " + s0_change_cost + ", " + real_s1_evaluate + ", " + s1_evaluate + ", " + estimated_cost1 + ", " + estimated_cost2);

            // } catch (Exception e) {
            //     LOG.warn("Failed SQL " + index + ": " + sql);
            //     _output.write("\nFailed to process this query.");
            // }
            } catch (Exception e) {
                LOG.warn("Failed SQL " + index + ": " + sql);
                LOG.warn("Error Message: " + e.getMessage(), e);
                _output.write("Failed SQL " + index + ": " + sql);
                _output.write("Error Message: " + e.getMessage());
            }
        }

        LOG.info("End of execution");
        return 0;
    }
}