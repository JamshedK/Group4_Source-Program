package sg.edu.nus.autotune;

import static org.slf4j.LoggerFactory.getLogger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.sql.rowset.CachedRowSet;
import com.sun.rowset.CachedRowSetImpl;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.slf4j.Logger;

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class PostgresDATA implements DataConnectivity {

    public static final String SELECT_FROM_EXPLAIN_NEW = "EXPLAIN (FORMAT TEXT, ANALYZE) ";

    private static Connection conn;

    private static final Logger LOG = getLogger("e2s2");

    private static final int _maxIndexLength = 2;

    private static List<String> _colNames;

    private static List<Set<Integer>> _columnsPerTable;

    private static List<List<Integer>> _allIndexes;

    private static List<BitSet> _coveredBy;

    private static List<BitSet> _covering;

    private static HashMap<Integer, String> _primaryKey;

    private static Set<Integer> _primaryCol;

    public static boolean bWFIT;

    public static String workload;
    private static Set<Index> _systemIndex;
    private static List<String> _columns;

    public PostgresDATA(boolean flag, String inputFile) throws Exception {
        bWFIT = flag;
        workload = inputFile;
    
        Class.forName("org.postgresql.Driver");
        conn = java.sql.DriverManager.getConnection(
            "jdbc:postgresql://localhost:5432/rCoreil", "teddyd", "dbms2025"
        );
        
        dropAllIndexes(); // Drop existing indexes
        extractInfo();   
    }
    

    public static void dropAllIndexes() throws SQLException {
        String dropAll = "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(dropAll);
        while (rs.next()) {
            String index = rs.getString("indexname");
            if (!index.toLowerCase().contains("pkey")) { // already checks
                String drop = "DROP INDEX IF EXISTS " + index;
                try (Statement dropStmt = conn.createStatement()) {
                    dropStmt.execute(drop);
                } catch (SQLException e) {
                    LOG.warn("Skipping index " + index + ": " + e.getMessage());
                }
            } else {
                LOG.info("Skipping primary key index: " + index);
            }
        }
        stmt.close();
    }
    

    // public static double execute(String sql) throws SQLException {
    //     LOG.info(sql + ";");
    //     if (sql.trim().toLowerCase().startsWith("select")) {
    //         return executeQuery(sql);
    //     } else {
    //         return executeUpdate(sql);
    //     }
    // }

    public static double execute(String sql) throws SQLException {
        LOG.info("Executing SQL: " + sql + ";");
    
        String trimmedSql = sql.trim().toLowerCase();
    
        if (trimmedSql.startsWith("select") || trimmedSql.startsWith("with")) {
            // queries starting with SELECT or WITH are reads
            return executeQuery(sql);
        } else {
            // everything else (UPDATE, INSERT, DELETE) is an update
            return executeUpdate(sql);
        }
    }
    

    private static double executeQuery(String sql) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql);
        double start = System.nanoTime();
        ps.execute();
        double end = System.nanoTime();
        ps.close();
        return (end - start) / 1_000_000.0;
    }

    private static double executeUpdate(String sql) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql);
        double start = System.nanoTime();
        ps.executeUpdate();
        double end = System.nanoTime();
        ps.close();
        return (end - start) / 1_000_000.0;
    }

    public static void explain(String sql) throws SQLException {
        String explainSQL = "EXPLAIN (ANALYZE, FORMAT TEXT) " + sql;
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(explainSQL);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        stmt.close();
    }

    public static BitSet getAddCandidateColumns(String sql) throws SQLException {
        BitSet rec = recommend(sql);
        BitSet cols = new BitSet();
        for (int i = rec.nextSetBit(0); i >= 0; i = rec.nextSetBit(i + 1)) {
            for (int j : _allIndexes.get(i)) {
                cols.set(j);
            }
        }
        return cols;
    }

    public static BitSet getAddCandidateBig(String sql, AbsConf s) throws SQLException {
        BitSet cand = new BitSet();
        BitSet candAddCol = getAddCandidateColumns(sql);
        for (int i = 0; i < _allIndexes.size(); i++) {
            if (!s.contains(i) && !_primaryKey.containsKey(i)) {
                if (candAddCol.get(_allIndexes.get(i).get(0))) {
                    cand.set(i);
                }
            }
        }
        return cand;
    }

    public static BitSet getAddCandidateBig(String sql) throws SQLException {
        BitSet cand = new BitSet();
        BitSet candAddCol = getAddCandidateColumns(sql);
        for (int i = 0; i < _allIndexes.size(); i++) {
            if (!_primaryKey.containsKey(i)) {
                if (candAddCol.get(_allIndexes.get(i).get(0))) {
                    cand.set(i);
                }
            }
        }
        return cand;
    }

    public static BitSet getAddCandidate(String sql, AbsConf s) throws SQLException {
        BitSet cand = getAddCandidate(sql);
        BitSet bs = s.toBitSet();
        for (int i = cand.nextSetBit(0); i >= 0; i = cand.nextSetBit(i + 1)) {
            if (bs.get(i)) {
                cand.set(i, false);
            }
        }
        return cand;
    }

    public static BitSet getAddCandidate(String sql) throws SQLException {
        BitSet cand = new BitSet();
        BitSet rec = recommend(sql);
        for (int i = rec.nextSetBit(0); i >= 0; i = rec.nextSetBit(i + 1)) {
            BitSet bs = _coveredBy.get(i);
            for (int j = bs.nextSetBit(0); j >= 0; j = bs.nextSetBit(j + 1)) {
                cand.set(j);
            }
        }
        return cand;
    }

    public static List<List<Integer>> getAllIndexes() {
        return _allIndexes;
    }

    public static String getColumnName(int i) {
        return _colNames.get(i);
    }

    public static BitSet getCoveredBy(int i) {
        return _coveredBy.get(i);
    }

    public static BitSet getCovering(int i) {
        return _covering.get(i);
    }

    public static CachedRowSet excute(String sql) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        CachedRowSetImpl crs = new CachedRowSetImpl();
        crs.populate(rs);
        stmt.close();
        return crs;
    }

    public static AbsConf getCurrentConfigration() throws SQLException {
        String sql = "SELECT indexname, tablename, indexdef FROM pg_indexes WHERE schemaname = 'public'";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        Set<Integer> conf = new HashSet<>();
        _primaryKey = new HashMap<>();
        _primaryCol = new HashSet<>();

        while (rs.next()) {
            String idxName = rs.getString("indexname");
            String tableName = rs.getString("tablename");
            String indexdef = rs.getString("indexdef");

            if (idxName.toLowerCase().contains("pkey")) {
                continue;
            }

            // crude extraction of column names from indexdef (e.g., 'CREATE INDEX ON tablename (col1, col2)')
            int openParen = indexdef.indexOf('(');
            int closeParen = indexdef.indexOf(')');
            if (openParen == -1 || closeParen == -1 || openParen >= closeParen) continue;
            String[] colNames = indexdef.substring(openParen + 1, closeParen).replaceAll("\\s", "").split(",");

            List<Integer> colIDs = new ArrayList<>();
            for (String str : colNames) {
                String colName = tableName + "." + str;
                if (_colNames.contains(colName)) {
                    colIDs.add(_colNames.indexOf(colName));
                }
            }

            int ID = _allIndexes.indexOf(colIDs);
            if (ID != -1) {
                conf.add(ID);
            }
        }

        _primaryKey.put(0, "EmptyIndex");
        conf.add(0);
        stmt.close();
        return new AbsConf(conf);
    }

    public static Set<Index> systemIndex() {
        return _systemIndex;
    }

    public static BitSet getDropCandidate(String sql, AbsConf s) throws SQLException {
        BitSet cand = new BitSet();
        SQLStatement sqls = new SQLStatement(sql);
        if (!sqls.getSQLCategory().isSame(SQLCategory.NOT_SELECT)) {
            return cand;
        }

        BitSet cols = getUpdatedCols(sql);
        BitSet bs = s.toBitSet();

        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
            if (isPrimaryKey(i)) {
                continue;
            }
            List<Integer> ints = _allIndexes.get(i);
            for (int j = cols.nextSetBit(0); j >= 0; j = cols.nextSetBit(j + 1)) {
                if (ints.contains(j)) {
                    cand.set(i);
                    break;
                }
            }
        }

        return cand;
    }

    public static BitSet getDropCandidate(String sql) throws SQLException {
        BitSet cand = new BitSet();
        SQLStatement sqls = new SQLStatement(sql);
        if (!sqls.getSQLCategory().isSame(SQLCategory.NOT_SELECT)) {
            return cand;
        }

        BitSet cols = getUpdatedCols(sql);
        for (int i = 0; i < _allIndexes.size(); i++) {
            if (isPrimaryKey(i)) {
                continue;
            }
            List<Integer> ints = _allIndexes.get(i);
            for (int j = cols.nextSetBit(0); j >= 0; j = cols.nextSetBit(j + 1)) {
                if (ints.contains(j)) {
                    cand.set(i);
                    break;
                }
            }
        }

        return cand;
    }

    public static int getMaxIndexLength() {
        return _maxIndexLength;
    }

    public static int getNumOfIndexes() {
        return _allIndexes.size();
    }

    public static String getPrimaryKeyName(int i) {
        if (isPrimaryKey(i)) {
            return _primaryKey.get(i);
        }
        throw new RuntimeException("Index " + i + " is not primary key!");
    }

    public static BitSet getUpdatedCols(String sql) throws SQLException {
        return obtainAffectCols(sql);
    }

    public static boolean isPrimaryKey(int i) {
        return _primaryKey.containsKey(i);
    }

    public static Set<Integer> getPrimaryCol() {
        return _primaryCol;
    }

    public static BitSet obtainAffectCols(String sql) throws SQLException {
        BitSet list = new BitSet();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("EXPLAIN (FORMAT JSON) " + sql);

        while (rs.next()) {
            String plan = rs.getString(1);
            for (String colName : _colNames) {
                if (plan.contains(colName)) {
                    list.set(_colNames.indexOf(colName));
                }
            }
        }

        stmt.close();
        return list;
    }

    public static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();

        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) {
                System.out.print("\t");
            }
            System.out.print(rsmd.getColumnName(i));
        }
        System.out.println();

        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) {
                    System.out.print(",  ");
                }
                String columnValue = rs.getString(i);
                System.out.print(columnValue);
            }
            System.out.println();
        }
    }

    public static BitSet recommend(String sql) throws SQLException {
        BitSet rec = new BitSet();
        BitSet usedCols = getUpdatedCols(sql);
        for (int i = 0; i < _allIndexes.size(); i++) {
            for (int colIdx : _allIndexes.get(i)) {
                if (usedCols.get(colIdx)) {
                    rec.set(i);
                    break;
                }
            }
        }
        return rec;
    }

    public static double whatif_getAddIndexCost(AbsConf s, int i) throws SQLException {
        if (s.contains(i)) {
            throw new RuntimeException("This Conf already contains index ID " + i + "!");
        }
        if (isPrimaryKey(i)) {
            throw new RuntimeException("Index ID " + i + " is primary key!");
        }
        return whatif_getExecCost(workload, s.toSetIndex());
    }

    public static double whatif_getAddIndexCost(Set<Index> indexes, Index index) throws SQLException {
        return whatif_getExecCost(workload, indexes);
    }

    public static double whatif_getDropIndexCost(AbsConf s, int i) throws SQLException {
        if (!s.contains(i)) {
            throw new RuntimeException("This Conf does not contain index ID " + i + "!");
        }
        if (isPrimaryKey(i)) {
            throw new RuntimeException("Index ID " + i + " is primary key!");
        }
        return whatif_getExecCost(workload, s.toSetIndex());
    }

    public static double whatif_getDropIndexCost(Set<Index> indexes, Index index) throws SQLException {
        return whatif_getExecCost(workload, indexes);
    }

    public static double whatif_getExecCost(String sql, AbsConf s) throws SQLException {
        return whatif_getExecCost(sql, s.toSetIndex());
    }

    public static double whatif_getExecCost(String sql, Set<Index> conf) throws SQLException {
        String explainSQL = "EXPLAIN (FORMAT JSON, ANALYZE) " + sql;
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(explainSQL);

        double cost = 0;
        while (rs.next()) {
            String plan = rs.getString(1);
            if (plan.contains("Total Cost")) {
                int idx = plan.indexOf("Total Cost");
                int end = plan.indexOf(',', idx);
                String costStr = plan.substring(idx, end);
                String[] tokens = costStr.split(":");
                if (tokens.length == 2) {
                    cost = Double.parseDouble(tokens[1].trim());
                }
            }
        }
        stmt.close();
        return cost;
    }

    public static List<Set<Integer>> getColumnsPerTable() {
        return _columnsPerTable;
    }

    private Random ran = new Random(1);
    protected boolean isLoadEnvironmentParameter = false;

    // public PostgresDATA(boolean flag, String str) throws Exception {
    //     bWFIT = flag;
    //     workload = str;
    //     dropAllIndexes();
    //     extractInfo();
    // }
    private void getEnvironmentParameters() throws Exception {
        if (isLoadEnvironmentParameter) {
            return;
        }
        // PostgreSQL does not use dbtune for metadata initialization like DB2.
        // All necessary setup is handled manually or via extractInfo()
        LOG.info("Environment parameters setup is skipped for PostgreSQL.");
        isLoadEnvironmentParameter = true;
    }    

    private void extractInfo() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet tablesRS = meta.getTables(null, "public", "%", new String[]{"TABLE"});
    
        _columns = new ArrayList<>();
        _colNames = new ArrayList<>();
        _columnsPerTable = new ArrayList<>();
        int column_index = 0;
    
        while (tablesRS.next()) {
            String tableName = tablesRS.getString("TABLE_NAME");
            ResultSet colsRS = meta.getColumns(null, "public", tableName, "%");
            Set<Integer> columnsSet = new HashSet<>();
            while (colsRS.next()) {
                String colName = colsRS.getString("COLUMN_NAME");
                _colNames.add(tableName + "." + colName);
                columnsSet.add(column_index);
                column_index++;
            }
            _columnsPerTable.add(columnsSet);
        }
    
        System.out.println("[extractInfo] Number of tables found: " + _columnsPerTable.size());
        System.out.println("[extractInfo] Number of columns found: " + _colNames.size());
    
        _allIndexes = generateAllIndexes(_columnsPerTable);
    
        System.out.println("[extractInfo] Number of candidate indexes generated: " + _allIndexes.size());
    
        _coveredBy = genCoveredBy();
        _covering = genCovering();
        _systemIndex = new HashSet<>();
        getCurrentConfigration();
    }
    

    private List<BitSet> genCoveredBy() {
        List<BitSet> coveredBy = new ArrayList<>();
        for (int i = 0; i < _allIndexes.size(); i++) {
            BitSet bs = new BitSet();
            for (int j = 0; j <= i; j++) {
                if (Tools.IsCovered(_allIndexes.get(j), _allIndexes.get(i))) {
                    bs.set(j);
                }
            }
            coveredBy.add(i, bs);
        }
        return coveredBy;
    }

    private List<BitSet> genCovering() {
        List<BitSet> covering = new ArrayList<>();
        for (int i = 0; i < _allIndexes.size(); i++) {
            BitSet bs = new BitSet();
            for (int j = i; j < _allIndexes.size(); j++) {
                if (Tools.IsCovering(_allIndexes.get(j), _allIndexes.get(i))) {
                    bs.set(j);
                }
            }
            covering.add(i, bs);
        }
        return covering;
    }

    private List<List<Integer>> generateAllIndexes(List<Set<Integer>> LSI) {
        Set<List<Integer>> SLI = new HashSet<>();
        for (Set<Integer> SI0 : LSI) {
            Set<Set<Integer>> PS = Sets.powerSet(SI0);
            for (Set<Integer> SI1 : PS) {
                if (SI1.size() > _maxIndexLength) {
                    continue;
                }
                List<Integer> LI1 = new ArrayList<>(SI1);
                Collection<List<Integer>> PLI1 = Collections2.permutations(LI1);
                for (List<Integer> LI2 : PLI1) {
                    if (LI2.size() <= _maxIndexLength) {
                        SLI.add(LI2);
                    }
                }
            }
        }
        List<List<Integer>> LLI = new ArrayList<>(SLI);
        Collections.sort(LLI, new CustomComparator());
        return LLI;
    }
    public static String getWorkloadName() {
        return workload;
    }
    
    public static String getWorkloadsFoldername() {
        return "workloads/";
    }

    public static Connection getConnection() {
        return conn;
    }    
    
}

