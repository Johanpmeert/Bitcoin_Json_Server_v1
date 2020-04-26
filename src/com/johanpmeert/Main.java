package com.johanpmeert;

/*
    How to call this server with HTTP GET:

    http://localhost:8080/balance/BTC=1Gvu3FxT7uUYWkbDUuMQwZ6e8DLtjv8ZhC                                 (legacy, segwit and bech32 addresses are allowed)
    http://localhost:8080/balance/BCH=bitcoincash:qzhtkhm9dakewnpx6uwd56e3m8v07h2ryqqr0mkkft             (or also legacy)
    http://localhost:8080/balance/BSV=1Gvu3FxT7uUYWkbDUuMQwZ6e8DLtjv8ZhC
    http://localhost:8080/balance/BTG=GZmpTPHQ6m5qbDtWQr1XNKSY3P8jjaYBrX                                 (or also legacy)

    Response JSON format:

    {"balance":0.00000000,"currency":"BTC","blockheight":610124,"errortype":"none"}

    formatted this gives:
        {
        "balance": 0.00000000,
        "currency": "BTC",
        "blockheight": 610124,
        "errortype": "none"
        }

    where:
    balance: balance of the coin or 0.0 when error
    currency: BTC, BCH, BTG or not_a_valid_coin
    blockheight: blockheight of the server returning balance
    errortype: can be:
        "none"
        "invalidaddress"
        "serverdown"
        "not_a_valid_coin"

    Note: Before using the balance:
    check if errortype ="none" otherwise balance is always 0
    check if blockheight is ok for this particular coin
*/

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.Headers;
import wf.bitcoin.javabitcoindrpcclient.BitcoinJSONRPCClient;

import java.net.InetSocketAddress;
import java.sql.*;
import java.time.LocalDateTime;

public class Main {
    // Http server details:
    private static final String HOSTNAME = "localhost";
    private static final int PORT = 8080;
    private static final int BACKLOG = 1;
    private static final String HEADER_ALLOW = "Allow";
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final int STATUS_OK = 200;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;
    private static final int NO_RESPONSE_LENGTH = -1;
    private static final String METHOD_GET = "GET";
    private static final String METHOD_OPTIONS = "OPTIONS";
    private static final String ALLOWED_METHODS = METHOD_GET + "," + METHOD_OPTIONS;
    // mysql details:
    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String DB_URL_1 = "jdbc:mysql://localhost:3306/";
    // private final String DB_URL1 = "jdbc:mysql://192.168.2.152:3306/";
    private static final String DB_URL_3 = "?serverTimezone=UTC";
    private static final String USER = "btcuser";
    private static final String PASS = "btcpass";
    private static final BigDecimal MINUS_ONE = BigDecimal.valueOf(-1.0);
    // node details below:
    private static final String PROTOCOL = "http";

    enum NodeDetails {
        BTC("jswemmelbtc", "ZEHUGANovjdw7jJUuZZr1L2sa", "192.168.2.16", 8332),
        BCH("johan", "4E1tr5lvU", "192.168.2.9", 8332),
        BSV("johan", "uFJpoKDJAxFo", "192.168.2.157", 8332),
        BTG("johan", "pci8VNgiMoyC", "192.168.2.10", 8332);

        public final String RPC_USER;
        public final String RPC_PASSWORD;
        public final String RPC_ADDRESS;
        public final int RPC_PORT;

        NodeDetails(String RPC_USER, String RPC_PASSWORD, String RPC_ADDRESS, int RPC_PORT) {
            this.RPC_USER = RPC_USER;
            this.RPC_PASSWORD = RPC_PASSWORD;
            this.RPC_ADDRESS = RPC_ADDRESS;
            this.RPC_PORT = RPC_PORT;
        }
    }

    enum CoinType {
        BTC("BTC"), BCH("BCH"), BSV("BSV"), BTG("BTG"), EMPTY("not_a_valid_coin");

        public final String Label;

        CoinType(String label) {
            this.Label = label;
        }
    }

    enum BitcoinErrorType {NONE, INVALID_ADDRESS, NOT_A_VALID_COIN, DB_SERVER_DOWN;}

    private static class HttpReturnJson {

        public BigDecimal balance;
        public CoinType currency;
        public int blockheight;
        public BitcoinErrorType errortype;

        HttpReturnJson() {
            balance = BigDecimal.ZERO;
            currency = CoinType.EMPTY;
            blockheight = 1;
            errortype = BitcoinErrorType.NONE;
        }
    }

    private static class RequestData {

        private CoinType coinType;
        private String address;
        private boolean invalid;

        public RequestData() {
            address = "";
            coinType = CoinType.EMPTY;
            invalid = false;
        }

        public CoinType getCoinType() {
            return coinType;
        }

        public String getAddress() {
            return address;
        }

        public boolean getInvalid() {
            return invalid;
        }

        public void setCoinType(CoinType ct) {
            this.coinType = ct;
        }

        public void setAddress(String adr) {
            this.address = adr;
        }

        public void setInvalid(boolean invalid) {
            this.invalid = invalid;
        }
    }

    private static class ResultFromGetCoinValue {
        public BigDecimal balance;
        public int blockHeight;

        ResultFromGetCoinValue() {
            balance = BigDecimal.ZERO;
            blockHeight = 0;
        }
    }


    public static void main(final String... args) throws IOException {
        System.out.print("Creating server... ");
        final HttpServer server = HttpServer.create(new InetSocketAddress(HOSTNAME, PORT), BACKLOG);
        System.out.println("done");
        server.createContext("/balance", myHttpHandler -> {
            try {
                final Headers headers = myHttpHandler.getResponseHeaders();
                final String requestMethod = myHttpHandler.getRequestMethod().toUpperCase();
                switch (requestMethod) {
                    case METHOD_GET:
                        long startTime = System.nanoTime();
                        StringBuilder consoleResponse = new StringBuilder();
                        consoleResponse.setLength(0);
                        RequestData requestData = getRequestParameters(myHttpHandler.getRequestURI().toString());
                        consoleResponse.append("Request at " + LocalDateTime.now() + ": " + requestData.getCoinType().Label + " = " + requestData.getAddress());
                        HttpReturnJson httpReturnJson = new HttpReturnJson();
                        httpReturnJson.currency = requestData.getCoinType();
                        if (requestData.getCoinType() == CoinType.EMPTY) {
                            httpReturnJson.errortype = BitcoinErrorType.NOT_A_VALID_COIN;
                        } else if ((requestData.getInvalid()) || (!isBtAddressValid(requestData))) {
                            httpReturnJson.errortype = BitcoinErrorType.INVALID_ADDRESS;
                        } else {
                            consoleResponse.append(", valid, getting balance... ");
                            ResultFromGetCoinValue result = getCoinValue(requestData);
                            consoleResponse.append(" done, = " + result.balance.toString());
                            httpReturnJson.blockheight = result.blockHeight;
                            if (result.balance.equals(MINUS_ONE)) {
                                httpReturnJson.errortype = BitcoinErrorType.DB_SERVER_DOWN;
                            } else {
                                httpReturnJson.balance = result.balance;
                            }
                        }
                        Gson gson = new Gson();
                        String responseBody = gson.toJson(httpReturnJson); // convert my object toJSON and then toString
                        consoleResponse.append(", response sent: " + responseBody);
                        long endTime = System.nanoTime();
                        consoleResponse.append(", that took " + (endTime - startTime) / 1e9 + "sec");
                        System.out.println(consoleResponse);
                        // send response back
                        headers.set(HEADER_CONTENT_TYPE, String.format("application/json; charset=%s", CHARSET));
                        final byte[] rawResponseBody = responseBody.getBytes(CHARSET);
                        myHttpHandler.sendResponseHeaders(STATUS_OK, rawResponseBody.length);
                        myHttpHandler.getResponseBody().write(rawResponseBody);
                        break;
                    case METHOD_OPTIONS:
                        headers.set(HEADER_ALLOW, ALLOWED_METHODS);
                        myHttpHandler.sendResponseHeaders(STATUS_OK, NO_RESPONSE_LENGTH);
                        break;
                    default:
                        headers.set(HEADER_ALLOW, ALLOWED_METHODS);
                        myHttpHandler.sendResponseHeaders(STATUS_METHOD_NOT_ALLOWED, NO_RESPONSE_LENGTH);
                        break;
                }
            } finally {
                myHttpHandler.close();
            }
        });
        System.out.print("Starting server... ");
        server.start();
        System.out.println("done");
        System.out.println("Listening to requests ...");
    }

    private static RequestData getRequestParameters(String requestString) {
        // This method sanitizes the input to prevent SQL injection
        RequestData requestData = new RequestData();
        if ((requestString == null) || (requestString.length() > 100)) {  // crap or attempt for SQL injection
            requestData.setInvalid(true);
            return requestData;
        }
        if (requestString.contains(CoinType.BTC.Label)) requestData.setCoinType(CoinType.BTC);
        else if (requestString.contains(CoinType.BCH.Label)) requestData.setCoinType(CoinType.BCH);
        else if (requestString.contains(CoinType.BSV.Label)) requestData.setCoinType(CoinType.BSV);
        else if (requestString.contains(CoinType.BTG.Label)) requestData.setCoinType(CoinType.BTG);
        if (requestString.contains("=")) {
            final String address = requestString.substring(requestString.indexOf('=') + 1);
            if (address.matches("-?[:=0-9a-zA-Z]+")) {
                requestData.setAddress(address);
            } else {
                requestData.setInvalid(true);
            }
        } else {
            requestData.setInvalid(true);
        }
        return requestData;
    }

    private static boolean isBtAddressValid(RequestData requestData) {
        // opening connection to correct bitnode
        try {
            URL rpcUrl;
            switch (requestData.getCoinType()) {
                case BCH:
                    rpcUrl = new URL(PROTOCOL + "://" + NodeDetails.BCH.RPC_USER + ":" + NodeDetails.BCH.RPC_PASSWORD + "@" + NodeDetails.BCH.RPC_ADDRESS + ":" + NodeDetails.BCH.RPC_PORT + "/");
                    break;
                case BSV:
                    rpcUrl = new URL(PROTOCOL + "://" + NodeDetails.BSV.RPC_USER + ":" + NodeDetails.BSV.RPC_PASSWORD + "@" + NodeDetails.BSV.RPC_ADDRESS + ":" + NodeDetails.BSV.RPC_PORT + "/");
                    break;
                case BTG:
                    rpcUrl = new URL(PROTOCOL + "://" + NodeDetails.BTG.RPC_USER + ":" + NodeDetails.BTG.RPC_PASSWORD + "@" + NodeDetails.BTG.RPC_ADDRESS + ":" + NodeDetails.BTG.RPC_PORT + "/");
                    break;
                default:
                    rpcUrl = new URL(PROTOCOL + "://" + NodeDetails.BTC.RPC_USER + ":" + NodeDetails.BTC.RPC_PASSWORD + "@" + NodeDetails.BTC.RPC_ADDRESS + ":" + NodeDetails.BTC.RPC_PORT + "/");
            }
            BitcoinJSONRPCClient bitcoinClient = new BitcoinJSONRPCClient(rpcUrl);
            boolean isvalid = bitcoinClient.validateAddress(requestData.getAddress()).isValid();
            return isvalid;
        } catch (MalformedURLException e) {
            return false;
        }
    }


    private static ResultFromGetCoinValue getCoinValue(RequestData requestData) {
        String DB_URL2 = "";
        ResultFromGetCoinValue result = new ResultFromGetCoinValue();
        switch (requestData.getCoinType()) {
            case BTC:
                DB_URL2 = "btc2";
                break;
            case BCH:
                DB_URL2 = "bch2";
                break;
            case BSV:
                DB_URL2 = "bsv";
                break;
            case BTG:
                DB_URL2 = "gold";
        }
        String DB_URL = DB_URL_1 + DB_URL2 + DB_URL_3;
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pst = null;
        try {
            Class.forName(JDBC_DRIVER);
            try {
                conn = DriverManager.getConnection(DB_URL, USER, PASS);
                try {
                    pst = conn.prepareStatement("set @adr=?");
                    pst.setString(1,requestData.getAddress());
                    pst.executeUpdate();
                    stmt = conn.createStatement();
                    stmt.setQueryTimeout(0);
                    String sql = "drop table if exists sum0;";
                    stmt.executeUpdate(sql);
                    sql = "create table sum0 like registration;";
                    stmt.executeUpdate(sql);
                    sql = "insert into sum0 select * from registration where btcaddress=@adr;";
                    stmt.executeUpdate(sql);
                    sql = "drop table if exists sum1;";
                    stmt.executeUpdate(sql);
                    sql = "create table sum1 like registration;";
                    stmt.executeUpdate(sql);
                    sql = "insert into sum1 select registration.* from registration inner join sum0 on registration.txid=sum0.txid;";
                    stmt.executeUpdate(sql);
                    sql = "delete from sum1 where btcaddress<>@adr and btcaddress<>'';";
                    stmt.executeUpdate(sql);
                    sql = "delete t1 from sum1 t1 inner join sum1 t2 where t1.txid=t2.txid and t1.vinvoutnr=t2.vinvoutnr and t1.vin<>t2.vin;";
                    stmt.executeUpdate(sql);
                    sql = "select sum(netvalue) as total from sum1 where btcaddress=@adr;";
                    ResultSet rs = stmt.executeQuery(sql);
                    if (rs.next()) {
                        BigDecimal testbalance = rs.getBigDecimal("total");
                        if (testbalance != null) result.balance = testbalance;
                    }
                    sql = "drop table sum0;";
                    stmt.executeUpdate(sql);
                    sql = "drop table sum1;";
                    stmt.executeUpdate(sql);
                    sql = "select blocknr from progress;";
                    rs = stmt.executeQuery(sql);
                    if (rs.next()) result.blockHeight = rs.getInt("blocknr");
                    return result;
                } catch (SQLException e) {
                    System.out.print(e);
                } finally {
                    conn.close();
                }
            } catch (SQLException se) {
                System.out.print(se);
            }
        } catch (ClassNotFoundException se) {
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException se2) {
            }
            try {
                if (pst != null) pst.close();
            } catch (SQLException se3) {
            }
            try {
                if (conn != null) conn.close();
            } catch (SQLException se) {
            }
        }
        result.balance = MINUS_ONE;
        return result;
    }

}





