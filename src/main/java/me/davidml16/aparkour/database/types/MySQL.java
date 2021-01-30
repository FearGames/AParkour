package me.davidml16.aparkour.database.types;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import me.davidml16.aparkour.Main;
import me.davidml16.aparkour.data.LeaderboardEntry;
import me.davidml16.aparkour.managers.ColorManager;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class MySQL implements Database {

    private HikariDataSource hikari;

    private String host, user, password, database;
    private int port;
    private int poolSize;

    private Main main;

    public MySQL(Main main) {
        this.main = main;
        this.host = main.getConfig().getString("MySQL.Host");
        this.user = main.getConfig().getString("MySQL.User");
        this.password = main.getConfig().getString("MySQL.Password");
        this.database = main.getConfig().getString("MySQL.Database");
        this.port = main.getConfig().getInt("MySQL.Port");
        this.poolSize = main.getConfig().getInt("MySQL.PoolSize", 10);
    }

    @Override
    public void close() {
        if (hikari != null) {
            hikari.close();
        }
    }

    @Override
    public void open() {
        if (hikari != null) return;

        try {
            HikariConfig config = new HikariConfig();
            config.setPoolName("AParkour Pool");
            config.setJdbcUrl("jdbc:mysql://" + this.host + ":" + this.port + "/" + this.database);
            config.setUsername(this.user);
            config.setPassword(this.password);
            config.setMaximumPoolSize(this.poolSize);
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            hikari = new HikariDataSource(config);

            Main.log.sendMessage(ColorManager.translate("    &aMySQL has been enabled!"));
        } catch (HikariPool.PoolInitializationException e) {
            Main.log.sendMessage(ColorManager.translate("    &cMySQL has an error on the conection! Now trying with SQLite..."));
            main.getDatabase().changeToSQLite();
        }
    }

    public void loadTables() {
        try (Connection connection = hikari.getConnection()) {
            String sql = "CREATE TABLE IF NOT EXISTS ap_times (`UUID` varchar(40) NOT NULL, `parkourID` varchar(25) NOT NULL, `lastTime` bigint NOT NULL, `bestTime` bigint NOT NULL, PRIMARY KEY (`UUID`, `parkourID`));";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try (Connection connection = hikari.getConnection()) {
            String sql = "CREATE TABLE IF NOT EXISTS ap_playernames (`UUID` varchar(40) NOT NULL, `NAME` varchar(40), PRIMARY KEY (`UUID`));";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void deleteParkourRows(String parkour) {
        Bukkit.getScheduler().runTaskAsynchronously(main, () -> {
            try (Connection connection = hikari.getConnection()) {
                String sql = "DELETE FROM ap_times WHERE parkourID = '" + parkour + "'";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.execute();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    public boolean hasData(UUID uuid, String parkour) throws SQLException {
        try (Connection connection = hikari.getConnection()) {
            String sql = "SELECT * FROM ap_times WHERE UUID = '" + uuid.toString() + "' AND parkourID = '" + parkour + "';";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public void createData(UUID uuid, String parkour) throws SQLException {
        try (Connection connection = hikari.getConnection()) {
            String sql = "REPLACE INTO ap_times (UUID,parkourID,lastTime,bestTime) VALUES(?,?,0,0)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, uuid.toString());
                statement.setString(2, parkour);
                statement.executeUpdate();
            }
        }
    }

    public boolean hasName(Player p) throws SQLException {
        try (Connection connection = hikari.getConnection()) {
            String sql = "SELECT * FROM ap_playernames WHERE UUID = '" + p.getUniqueId().toString() + "';";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public void updatePlayerName(Player p) {
        Bukkit.getScheduler().runTaskAsynchronously(main, () -> {
            try (Connection connection = hikari.getConnection()) {
                String sql = "REPLACE INTO ap_playernames (UUID,NAME) VALUES(?,?)";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, p.getUniqueId().toString());
                    statement.setString(2, p.getName());
                    statement.executeUpdate();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    public String getPlayerName(String uuid) throws SQLException {
        try (Connection connection = hikari.getConnection()) {
            String sql = "SELECT * FROM ap_playernames WHERE UUID = '" + uuid + "';";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next()) {
                        return result.getString("NAME");
                    }
                }
            }
        }
        return "";
    }

    public String getPlayerUUID(String name) throws SQLException {
        try (Connection connection = hikari.getConnection()) {
            String sql = "SELECT * FROM ap_playernames WHERE NAME = '" + name + "';";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next()) {
                        return result.getString("UUID");
                    }
                }
            }
        }
        return "";
    }

    public Long getLastTime(UUID uuid, String parkour) throws SQLException {
        try (Connection connection = hikari.getConnection()) {
            String sql = "SELECT * FROM ap_times WHERE UUID = '" + uuid.toString() + "' AND parkourID = '" + parkour + "';";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next()) {
                        return result.getLong("lastTime");
                    }
                }
            }
        }
        return 0L;
    }

    public Long getBestTime(UUID uuid, String parkour) throws SQLException {
        try (Connection connection = hikari.getConnection()) {
            String sql = "SELECT * FROM ap_times WHERE UUID = '" + uuid.toString() + "' AND parkourID = '" + parkour + "';";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next()) {
                        return result.getLong("bestTime");
                    }
                }
            }
        }
        return 0L;
    }

    public void setTimes(UUID uuid, Long lastTime, Long bestTime, String parkour) {
        Bukkit.getScheduler().runTaskAsynchronously(main, () -> {
            try (Connection connection = hikari.getConnection()) {
                String sql = "REPLACE INTO ap_times (UUID,parkourID,lastTime,bestTime) VALUES(?,?,?,?)";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, uuid.toString());
                    statement.setString(2, parkour);
                    statement.setLong(3, lastTime);
                    statement.setLong(4, bestTime);
                    statement.executeUpdate();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    public CompletableFuture<Map<String, Long>> getPlayerLastTimes(UUID uuid) {
        CompletableFuture<Map<String, Long>> result = new CompletableFuture<>();
        Map<String, Long> times = new HashMap<>();
        Bukkit.getScheduler().runTaskAsynchronously(main, () -> {
            for (File file : Objects.requireNonNull(new File(main.getDataFolder(), "parkours").listFiles())) {
                String parkour = file.getName().replace(".yml", "");
                try {
                    if (hasData(uuid, parkour)) {
                        times.put(parkour, getLastTime(uuid, parkour));
                    } else {
                        createData(uuid, parkour);
                        times.put(parkour, 0L);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            result.complete(times);
        });
        return result;
    }

    public CompletableFuture<Map<String, Long>> getPlayerBestTimes(UUID uuid) {
        CompletableFuture<Map<String, Long>> result = new CompletableFuture<>();
        Map<String, Long> times = new HashMap<>();
        Bukkit.getScheduler().runTaskAsynchronously(main, () -> {
            for (File file : Objects.requireNonNull(new File(main.getDataFolder(), "parkours").listFiles())) {
                String parkour = file.getName().replace(".yml", "");
                try {
                    if (hasData(uuid, parkour)) {
                        times.put(parkour, getBestTime(uuid, parkour));
                    } else {
                        createData(uuid, parkour);
                        times.put(parkour, 0L);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            result.complete(times);
        });
        return result;
    }

    public CompletableFuture<List<LeaderboardEntry>> getParkourBestTimes(String id, int amount) {
        CompletableFuture<List<LeaderboardEntry>> result = new CompletableFuture<>();
        List<LeaderboardEntry> times = new ArrayList<>();

        Bukkit.getScheduler().runTaskAsynchronously(main, () -> {
            long start = System.currentTimeMillis();
            try (Connection connection = hikari.getConnection()) {
                String sql = "SELECT ap_times.*, ap_playernames.NAME FROM ap_times JOIN ap_playernames ON ap_times.UUID = ap_playernames.UUID WHERE bestTime != 0 AND parkourID = '" + id + "' ORDER BY bestTime ASC LIMIT " + amount + ";";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    try (ResultSet rs = statement.executeQuery()) {
                        while (rs.next()) {
                            UUID uuid = UUID.fromString(rs.getString("UUID"));
                            if (main.getLeaderboardHandler().getPlayerNames().containsKey(uuid)) {
                                times.add(new LeaderboardEntry(main.getLeaderboardHandler().getPlayerNames().get(uuid), rs.getLong("bestTime")));
                            } else {
                                String playerName = rs.getString("NAME");
                                times.add(new LeaderboardEntry(playerName, rs.getLong("bestTime")));
                                main.getLeaderboardHandler().getPlayerNames().put(uuid, playerName);
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            Bukkit.getScheduler().runTask(main, () -> result.complete(times));
        });
        return result;
    }
}
