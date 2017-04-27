package TwitterApp;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterApp {
	static String dbURL = "jdbc:mysql://localhost:3306/cs274";
	static String username = "root";
	static String password = "mysql";
	static Hashtable<String, Integer> ht_StopWords = new Hashtable<String, Integer>();

	static int candidature_threshold = 30;
	static double occurence_threshold = 3;
	static Hashtable<String, Integer> ht_SeedWords = new Hashtable<String, Integer>();

	static HashMap<String, HashMap<String, Integer>> hm_Association = new HashMap<String, HashMap<String, Integer>>();

	private static long startTime = System.currentTimeMillis();

	public static void main(String[] args) throws TwitterException, IOException, SQLException {

		List<Status> allStatuses = extractTwitterData();

		//populateStopWords();

		//insertProcessData(allStatuses);

		//buildAssociations();

		//classifyTweets();

		long endTime = System.currentTimeMillis();

		System.out.println("It took " + ((endTime - startTime) / 1000) + " seconds");
	}

	private static void insertProcessData(List<Status> allStatuses) {
		int rows_inserted = 0;

		try (Connection conn = DriverManager.getConnection(dbURL + "?useUnicode=true&characterEncoding=utf-8", username,
				password)) {

			Statement statementObj = conn.createStatement();

			statementObj.executeUpdate("SET FOREIGN_KEY_CHECKS = 0");
			statementObj.executeUpdate("Truncate table tbl_raw_tweets");
			statementObj.executeUpdate("SET FOREIGN_KEY_CHECKS = 1");
			statementObj.executeUpdate("ALTER TABLE tbl_raw_tweets AUTO_INCREMENT = 1");

			statementObj.executeUpdate("Truncate table tbl_processed_tweets");
			statementObj.executeUpdate("ALTER TABLE tbl_processed_tweets AUTO_INCREMENT = 1");

			for (Status a : allStatuses) {

				String sql = "INSERT INTO tbl_raw_tweets (tweet_id, username, tweet,created_at) VALUES (?, ?, ?, ?)";

				PreparedStatement statement = conn.prepareStatement(sql);
				statement.setLong(1, a.getId());
				statement.setString(2, a.getUser().getName());

				String cleanString = a.getText().replaceAll("\\P{Print}", "");

				statement.setString(3, cleanString);

				Timestamp created_at = new Timestamp(a.getCreatedAt().getTime());
				statement.setTimestamp(4, created_at);

				int returnValue = statement.executeUpdate();

				if (returnValue != 0) {
					rows_inserted++;
				}

				String processed_tweet = processRawTweets(cleanString.toLowerCase());

				String sql2 = "INSERT INTO tbl_processed_tweets (R_ID, processed_tweet, class) VALUES (?, ?, ?)";

				PreparedStatement statement2 = conn.prepareStatement(sql2);

				statement2.setLong(1, rows_inserted);
				statement2.setString(2, processed_tweet);
				statement2.setLong(3, -1);

				int returnValue2 = statement2.executeUpdate();
			}
		} catch (SQLException ex) {
			ex.printStackTrace();
		}

		System.out.println("Rows Inserted: " + rows_inserted);
	}

	private static List<Status> extractTwitterData() throws TwitterException {
		ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
		configurationBuilder.setDebugEnabled(true).setOAuthConsumerKey("7HV9y7SVQxbUh2oYAhWixQBbu")
				.setOAuthConsumerSecret("yyKgGiHynyFyRuCuPNOaTfciWak9i8mSuqEfSD7EByWCj4icGc")
				.setOAuthAccessToken("1650776516-rv36QTpAzWgCGWVNoH3ggoyAAf41OUWCAaZkv6z")
				.setOAuthAccessTokenSecret("ZYo7owYZj71OKpggFtMrizXoCtANzJAEZ1A7zSgXs3FSi");

		TwitterFactory tf = new TwitterFactory(configurationBuilder.build());
		Twitter twitter = tf.getInstance();

		List<Status> homeStatus = twitter.getHomeTimeline();
		List<Status> allStatuses = new ArrayList<Status>();

		List<Long> uniqueUsers = new ArrayList<>();

		// BufferedWriter out = new BufferedWriter(new
		// FileWriter("outputFile.txt"));

		for (Status s : homeStatus) {
			long userId = s.getUser().getId();

			if (uniqueUsers.contains(userId) == false) {
				uniqueUsers.add(userId);
				System.out.println(userId);
			}

		}

		System.out.println(uniqueUsers.size());

		for (long uniqueUserID : uniqueUsers) {

			int pageno = 1;
			List<Status> statuses = new ArrayList<Status>();

			while (true) {
				try {

					int size = statuses.size();
					Paging page = new Paging(pageno++, 100);
					statuses.addAll(twitter.getUserTimeline(uniqueUserID, page));
					if (statuses.size() == size)
						break;
				} catch (TwitterException e) {

					e.printStackTrace();
				}
			}

			for (Status u : statuses) {
				allStatuses.add(u);
			}
		}
		return allStatuses;
	}

	private static String processRawTweets(String raw_tweet) {
		String processed_tweet = "";

		try {
			processed_tweet = raw_tweet.replaceAll("https?://\\S+\\s?", "");
			processed_tweet = processed_tweet.replaceAll("http?://\\S+\\s?", "");
			processed_tweet = processed_tweet.replace(".", "");
			processed_tweet = processed_tweet.replace(",", "");
			processed_tweet = processed_tweet.replace("@", "");
			processed_tweet = processed_tweet.replace("!", "");
			processed_tweet = processed_tweet.replace("#", "");
			processed_tweet = processed_tweet.replace("$", "");
			processed_tweet = processed_tweet.replace(":", "");
			processed_tweet = processed_tweet.replace(";", "");
			processed_tweet = processed_tweet.replace("\"", "");
			processed_tweet = processed_tweet.replace("'", "");
			processed_tweet = processed_tweet.replace("/", "");
			processed_tweet = processed_tweet.replace("\\", "");
			processed_tweet = processed_tweet.replace("-", "");
			processed_tweet = processed_tweet.replace("_", "");
			processed_tweet = processed_tweet.replace("+", "");
			processed_tweet = processed_tweet.replace("=", "");
			processed_tweet = processed_tweet.replace("?", "");
			processed_tweet = processed_tweet.replace("%", "");
			processed_tweet = processed_tweet.replace("^", "");
			processed_tweet = processed_tweet.replace("<", "");
			processed_tweet = processed_tweet.replace(">", "");
			processed_tweet = processed_tweet.replace("rt", "");

			String[] words = processed_tweet.split("\\s");

			for (String s : words) {
				Object returnObj = new Object();
				returnObj = ht_StopWords.get(s);

				if (returnObj != null) {
					// Stop word found
					// String regex = "\\s*\\b"+s+"\\b\\s*";
					String regex = "\\b" + s + "\\b\\s*";
					processed_tweet = processed_tweet.replaceAll(regex, "");
				}
			}

		}

		catch (Exception ex) {
			System.out.println(ex.toString());
			processed_tweet = "";
		}

		return processed_tweet;

	}

	private static void populateStopWords() throws SQLException {
		Connection conn = DriverManager.getConnection(dbURL + "?useUnicode=true&characterEncoding=utf-8", username,
				password);
		try {

			String query = "SELECT stop_word FROM tbl_stop_words";

			// create the java statement
			Statement st = conn.createStatement();

			// execute the query, and get a java resultset
			ResultSet rs = st.executeQuery(query);

			// iterate through the java resultset
			while (rs.next()) {
				String stop_word = rs.getString("stop_word");

				ht_StopWords.put(stop_word, 1);
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			conn.close();
		}

	}

	private static void buildAssociations() throws SQLException {

		Connection conn = DriverManager.getConnection(dbURL + "?useUnicode=true&characterEncoding=utf-8", username,
				password);
		try {

			String query = "SELECT seedword FROM tbl_seed_words";
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);

			while (rs.next()) {
				String seedWord = rs.getString("seedword");
				hm_Association.put(seedWord, new HashMap<String, Integer>());
			}

			query = "Select processed_tweet from tbl_processed_tweets";
			st = conn.createStatement();
			rs = st.executeQuery(query);

			while (rs.next()) {
				String tweet = rs.getString("processed_tweet");

				String[] words = tweet.split("\\s");

				for (String w : words) {
					if (hm_Association.containsKey(w)) {

						// First return the entire Hashmap for the seedword
						// HashMap<String, Integer> returnHash = new
						// HashMap<String, Integer>();
						// returnHash = hm_Association.get(w);

						// Now for each of the words in the tweet check if they
						// are in the returnHash object.
						// If they are present, then just increase the count for
						// that candidate
						// If not present, then add an entry to the returnHash
						// object for the candidate
						// Remember NOT to consider the seed word as a potential
						// candidate

						for (String iW : words) {

							if (iW == w) {// To avoid seed word -seed word
											// association
								continue;
							}

							else {
								if (hm_Association.get(w).containsKey(iW)) {
									// Here the association for the candidate
									// and seed word already exists. We just
									// need to update the count
									Integer count = hm_Association.get(w).get(iW);
									count = (Integer) (count.intValue() + 1);

									// Now check if the seed word - candidate
									// association has surpassed the threshold
									// If it has surpassed, then the candidate
									// becomes a new seed word and add it to the
									// association hash map

									if (count.intValue() >= candidature_threshold) {
										// candidate becomes a new seed word
										hm_Association.put(iW, new HashMap<String, Integer>());// Adding
																								// the
																								// new
																								// seed
																								// word
										hm_Association.get(w).remove(iW);// Removing
																			// the
																			// old
																			// seed
																			// word-candidate
																			// association

									} else {
										// Update the count of seed
										// word-candidate association
										hm_Association.get(w).put(iW, count);

									}
								}

								else {
									// Here the association between seed word
									// and candidate does not exist. Hence we
									// have to add the association
									hm_Association.get(w).put(iW, new Integer(1));

								}
							}
						}

						break;// We need to break this so as not to consider all
								// the words in the tweet again as the
								// candidates
					}

				}
			}

			// Now truncate all the data in the tbl_seed_words and add all the
			// new seed words in the hm_Association to the table.

			st.executeUpdate("Truncate table tbl_seed_words");
			st.executeUpdate("ALTER TABLE tbl_seed_words AUTO_INCREMENT = 1");

			for (String key : hm_Association.keySet()) {
				query = "INSERT INTO tbl_seed_words (seedword) VALUES (?)";

				PreparedStatement statement2 = conn.prepareStatement(query);

				statement2.setString(1, key);

				statement2.executeUpdate();

			}
		}

		catch (Exception ex) {
			System.out.println(ex.getMessage());
		}

		finally {
			conn.close();
		}
	}

	private static void classifyTweets() throws SQLException {
		Connection conn = DriverManager.getConnection(dbURL + "?useUnicode=true&characterEncoding=utf-8", username,
				password);

		try {

			String query = "SELECT * FROM tbl_processed_tweets";
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);

			while (rs.next()) {
				String tweet = rs.getString("processed_tweet");
				int P_ID = rs.getInt("P_ID");

				String sql2 = "UPDATE tbl_processed_tweets SET class = ? WHERE P_ID = ?";

				PreparedStatement statement2 = conn.prepareStatement(sql2);

				if (getClass(tweet) == 0) {
					// classify as 0
					statement2.setLong(1, 0);
					statement2.setLong(2, P_ID);

				} else {
					// classify as 1
					statement2.setLong(1, 1);
					statement2.setLong(2, P_ID);
				}

				int returnValue2 = statement2.executeUpdate();

			}

		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			conn.close();
		}
	}

	private static double getSimilarity(String tweet) {

		double similarity = 0.0;
		int modA = hm_Association.size();
		int modB = 0;
		int modAUB = 0;
		int modAIB = 0;

		try {
			String[] words = tweet.split("\\s");
			modB = words.length;
			modAUB = modA + modB;

			for (String w : words) {
				if (hm_Association.containsKey(w)) {
					modAIB++;
				}
			}

			similarity = (double) modAIB / (double) modAUB;

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		return similarity;

	}

	private static int getClass(String tweet) {

		int result = 0;
		int count = 0;

		try {
			String[] words = tweet.split("\\s");

			for (String w : words) {
				if (hm_Association.containsKey(w)) {
					count++;

					if (count >= occurence_threshold) {
						result = 1;
						break;
					}

				}
			}

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		return result;
	}

}