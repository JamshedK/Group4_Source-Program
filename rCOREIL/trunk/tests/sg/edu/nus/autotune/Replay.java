package sg.edu.nus.autotune;

import java.sql.SQLException;
import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;
import sg.edu.nus.util.FileReader;

public class Replay extends Execution {

    private static final String RESULT_FILE_PREFIX = null;
    private static final Logger LOG = getLogger("e2s2");

    private PostgresDATA _data;

    public Replay(DataConnectivity data) throws SQLException {
        super(data);
        _data = (PostgresDATA) data;  
        PostgresDATA.dropAllIndexes();  
        if (RESULT_FILE_PREFIX == null || RESULT_FILE_PREFIX.isEmpty()) {
            _resultFilePrefix = DEFAULT_RESULT_FILE_PREFIX;
        } else {
            _resultFilePrefix = RESULT_FILE_PREFIX;
        }
    }

    @Override
    protected int run() throws Exception {

        LOG.info("# Replay with " + _data.getWorkloadName());
        _output.write("# Replay with " + _data.getWorkloadName());
        _output.write("# Conf_Change" + "\t" + "Exec_Time");

        int index = 0;
        double alg_time = 0;
        double real_s0_change_cost = 0;
        double real_s1_evaluate = 0;

        FileReader in = new FileReader(
            _data.getWorkloadsFoldername() + _data.getWorkloadName(), 
            null
        );

        String line = null;
        while ((line = in.readLine()) != null) {
            try {
                if (line.startsWith("Overhead")) {
                    alg_time = Double.parseDouble(line.substring(10));
                } else if (line.startsWith("CREATE") || line.startsWith("DROP")) {
                    real_s0_change_cost += PostgresDATA.execute(line.substring(0, line.length() - 1));
                } else {
                    ++index;
                    real_s1_evaluate = PostgresDATA.execute(line.substring(0, line.length() - 1));
                    _output.write(real_s0_change_cost + "\t" + real_s1_evaluate);
                    LOG.info("SQL " + index + " done");

                    alg_time = 0;
                    real_s0_change_cost = 0;
                }
            } catch (Exception e) {
                LOG.warn("Failed to execute line: " + line, e);
            }
        }

        LOG.info("End of execution");

        return 0;
    }
}
