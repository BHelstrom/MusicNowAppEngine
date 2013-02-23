package com.google.appengine.musicnow;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.android.gcm.server.Sender;
import com.google.appengine.api.rdbms.AppEngineDriver;
import com.shared.app.messages.AuthenticateUserLoginRequest;
import com.shared.app.messages.MusicNowMessage;
import com.shared.app.messages.MusicNowMessageType;
import com.shared.app.messages.MusicNowMessageType.Type;
import com.shared.app.messages.Response;
import com.shared.app.messages.RetrieveAccountResponse;
import com.shared.app.messages.UpdatePerformerAccountRequest;

@SuppressWarnings("serial")
public class MusicNowAppEngineServlet extends HttpServlet {

	private Sender sender;
	private static Connection connection = null;

	private PreparedStatement selectUserPasswordFromUserTable = null;
	private PreparedStatement selectUserEmailFromUserTable = null;
	private PreparedStatement insertIntoUserTable = null;
	private PreparedStatement updateUserTable = null;

	private PreparedStatement insertIntoAccountTable = null;
	private PreparedStatement updateAccountTable = null;
	
	private PreparedStatement selectAccountNumberFromUserTable = null;
	private PreparedStatement selectAccountTypeFromAccountTypeTable = null;

	private PreparedStatement insertIntoPerformerTable = null;
	private PreparedStatement updatePerformerTable = null;
	
	private PreparedStatement selectUserFromUserTable = null;
	private PreparedStatement selectAccountFromAccountTable = null;
	private PreparedStatement selectAccountType = null;
	private PreparedStatement selectPerformer = null;

	public MusicNowAppEngineServlet() {
		super();

		this.sender = new Sender("AIzaSyAjEwMRK8Ar9zC-2OhCRWnk6ME44pYdnKI");

		// connect to database
		openDatabaseConnection();

		// create prepared statements
		if (connection != null) {
			createPreparedStatements();
		}
	}

	private static void openDatabaseConnection() {
		try {
			// create new app engine driver
			DriverManager.registerDriver(new AppEngineDriver());

			// connect to database
			connection = DriverManager
					.getConnection("jdbc:google:rdbms://musicnow-team2:musicnow/MusicNowApp");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void closeDatabaseConnection() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void createPreparedStatements() {
		try {
			// retrieve password from user table
			this.selectUserPasswordFromUserTable = connection
					.prepareStatement("select * from User where Usrid = ? and Usrpassword = ?");

			// retrieve email address from user table
			this.selectUserEmailFromUserTable = connection
					.prepareStatement("select usrEmail from User where Usrid = ? and Usrpassword = ?");

			// insert into user table
			this.insertIntoUserTable = connection
					.prepareStatement("insert into User (Usrid, Usrpassword, usrEmail) values (?,?,?)");

			// update user table
			this.updateUserTable = connection
					.prepareStatement("update User set Usrpassword = ?, usrEmail = ? where Usrid = ?");

			// retrieve account number from user table
			this.selectAccountNumberFromUserTable = connection
					.prepareStatement("select UsrAccNumber from User where Usrid = ?");

			// retrieve account type from account type table
			this.selectAccountTypeFromAccountTypeTable = connection
					.prepareStatement("select AccTypeid from AccountType where AccTypedesc = ?");

			// insert into account table
			this.insertIntoAccountTable = connection
					.prepareStatement("insert into Account (AccNumber,AccType,AccName,AccDesc,AccWebsite,AccEventID,AccImage) values (?,?,?,?,?,?,?)");

			// update account table
			this.updateAccountTable = connection.prepareStatement("update Account set AccType = ?,AccName = ?,AccDesc = ?,AccWebsite = ?,AccEventID = ?,AccImage = ? where AccNumber = ?");
			
			// insert into the performer table
			this.insertIntoPerformerTable = connection
					.prepareStatement("insert into Performer(AccNumber,Pid,Genreid) values (?,?,?)");

			// update performer table
			this.updatePerformerTable = connection.prepareStatement("update Performer set Pid = ?,Genreid = ? where AccNumber = ?");

			
			// select the user account number, email from user table
			this.selectUserFromUserTable = connection
					.prepareStatement("select UsrAccNumber, usrEmail from User where Usrid = ? and Usrpassword = ?");

			// select the account information from account table
			this.selectAccountFromAccountTable = connection
					.prepareStatement("select AccType, AccName, AccDesc, AccWebsite, AccEventID, AccImage from Account where AccNumber = ?");

			// select the account type
			this.selectAccountType = connection
					.prepareStatement("select AccTypedesc from AccountType where AccTypeid = ?");

			// select the performer account
			this.selectPerformer = connection
					.prepareStatement("select Genreid from Performer where AccNumber = ?");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		processRequest(req, resp);
	}

	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		processRequest(req, resp);
	}

	protected boolean isEmptyOrNull(String value) {
		return value == null || value.trim().length() == 0;
	}

	protected String getParameter(HttpServletRequest req, String parameter)
			throws ServletException {
		String value = req.getParameter(parameter);
		if (isEmptyOrNull(value)) {
			throw new ServletException("Parameter " + parameter + " not found");
		}
		return value.trim();
	}

	private void processRequest(HttpServletRequest request,
			HttpServletResponse response) throws IOException {
		try {
			if (connection != null) {

				Type type = MusicNowMessageType.toType(request
						.getParameter(MusicNowMessage.TYPE));
				switch (type) {
				case AUTHENTICATE_USER_LOGIN_RQST:
					this.authenticateUserLogin(request, response);
					break;
				case UPDATE_PERFORMER_ACCOUNT:
					this.updatePerformerAccount(request, response);
				default:
					// response.getWriter().println("<br>No message type : " +
					// type);
				}
			}
		} catch (SQLException exception) {
			response.getWriter().println(
					"  MusicNow database connection failed: "
							+ exception.getMessage());
			exception.printStackTrace();
		} catch (Exception e) {
			response.getWriter().println("<br>exception : " + e.getMessage());
		}

	}

	private void authenticateUserLogin(HttpServletRequest request,
			HttpServletResponse response) throws SQLException, IOException,
			Exception {
		// retrieve the message
		AuthenticateUserLoginRequest rqst = new AuthenticateUserLoginRequest(
				request);
//response.getWriter().println("1,");
		// fill in the query with the username to retrieve the password
		this.selectUserPasswordFromUserTable.setString(1, rqst.getUsername());
		this.selectUserPasswordFromUserTable.setString(2, rqst.getPassword());

		// execute the query to retrieve the password
		ResultSet resultSet = this.selectUserPasswordFromUserTable
				.executeQuery();
//		response.getWriter().println("2,");
		Boolean status = Boolean.FALSE;
		if (resultSet.next()) {
			// login was successful, create account

			// retrieve user account number and email
			this.selectUserFromUserTable.setString(1, rqst.getUsername());
			this.selectUserFromUserTable.setString(2, rqst.getPassword());
			ResultSet set1 = this.selectUserFromUserTable.executeQuery();
			if (set1.next()) {
//				response.getWriter().println("3,");
				// retrieve account info from account table
				this.selectAccountFromAccountTable.setInt(1,
						set1.getInt("UsrAccNumber"));
				ResultSet set2 = this.selectAccountFromAccountTable
						.executeQuery();
				if (set2.next()) {
//					response.getWriter().println("4,");
					// retrieve the appropriate account type
					this.selectAccountType.setInt(1, set2.getInt("AccType"));
					ResultSet set3 = this.selectAccountType.executeQuery();
					if (set3.next()) {
//						response.getWriter().println("5,");
						// retrieve specific account information
						String accountType = set3.getString("AccTypedesc");
						if (accountType.equals("Performer")) {
							this.selectPerformer.setInt(1,
									set1.getInt("UsrAccNumber"));
							ResultSet set4 = this.selectPerformer
									.executeQuery();
							if (set4.next()) {
//								response.getWriter().println("6,");
								byte[] image = new byte[2];
								image[0] = set2.getByte("AccImage");
								RetrieveAccountResponse rsp = new RetrieveAccountResponse(
										rqst.getRegistrationID(),
										set3.getString("AccTypedesc"),
										rqst.getUsername(), rqst.getPassword(),
										set1.getString("usrEmail"),
										set2.getString("AccDesc"), image,
										set2.getInt("AccEventID"),
										set2.getString("AccWebsite"),
										set4.getInt("Genreid"));
								response.getWriter().println(rsp.getMessage());
								return;
							}
						} else if (accountType.equals("Venue")) {
							// this.selectVenue.
						}
					}

				}
			}
		}

		// return response
		Response rsp = new Response(rqst.getRegistrationID(), status);
		response.getWriter().println(rsp.getMessage());
	}

	private void retrieveUserLogin(HttpServletRequest request,
			HttpServletResponse response) throws SQLException, IOException {
		// fill in the query with the username to retrieve the email address
		// this.selectUserEmailFromUserTable.setString(1, user);
		//
		// // execute the query to retrieve the email
		// ResultSet resultSet =
		// this.selectUserEmailFromUserTable.executeQuery();
		//
		// while (resultSet.next()) {
		// response.getWriter().println(
		// "<br>Retrieve User Password, Email for User (" + user
		// + ") password = "
		// + resultSet.getString("Usrpassword") + ", email = "
		// + resultSet.getString("usrEmail"));
		// }
	}

	private void updatePerformerAccount(HttpServletRequest request,
			HttpServletResponse response) throws IOException {

		try {
			UpdatePerformerAccountRequest rqst = new UpdatePerformerAccountRequest(
					request);

			// see if account exists, if so, modify, if not, add it
			this.selectAccountNumberFromUserTable.setString(1,
					rqst.getUsername());
			ResultSet query = this.selectAccountNumberFromUserTable
					.executeQuery();
			if (query.next()) {
				response.getWriter().println("modifying account...");
				modifyPerformerAccount(rqst, response);
			} else {
				response.getWriter().println("adding new account...");
				addNewPerformerAccount(rqst, response);
			}
		} catch (Exception e) {
			Response rsp = new Response(
					request.getParameter(MusicNowMessage.REG_ID), Boolean.FALSE);
			response.getWriter().println(rsp.getMessage());
		}
	}

	private void addNewPerformerAccount(UpdatePerformerAccountRequest rqst,
			HttpServletResponse response) throws IOException {
		try {
			// add the performer account into the User table
			Integer accountNumber = this.insertIntoUserTable(
					rqst.getUsername(), rqst.getPassword(), rqst.getEmail());

			// retrieve the performer account type from AccountType table
			this.selectAccountTypeFromAccountTypeTable
					.setString(1, "Performer");
			ResultSet accountTypeResult = this.selectAccountTypeFromAccountTypeTable
					.executeQuery();
			Integer accountType = 0;
			while (accountTypeResult.next()) {
				accountType = accountTypeResult.getInt("AccTypeid");
				// response.getWriter().println(
				// "<br>Retrieve Performer account type = " + accountType);
			}

			// add the performer account into the Account table
			this.insertIntoAccountTable(accountNumber, accountType,
					rqst.getUsername(), rqst.getDescription(),
					rqst.getWebsite(), rqst.getEvent(), rqst.getImage());

			// add the performer account into the Performer table
			this.insertIntoPerformerTable.setInt(1, accountNumber);
			this.insertIntoPerformerTable.setInt(2, accountNumber);
			this.insertIntoPerformerTable.setInt(3, rqst.getGenre());
			Integer result = this.insertIntoPerformerTable.executeUpdate();
			if (result != 1) {
				throw new Exception("Failed to insert into performer table");
			}
			// response.getWriter()
			// .println("<br>Add Performer result = " + result);
			Response rsp = new Response(rqst.getRegistrationID(), Boolean.TRUE);
			response.getWriter().println(rsp.getMessage());
		} catch (Exception e) {
			// return response
			// response.getWriter().println("threw an error: " +
			// e.getMessage());
			Response rsp = new Response(rqst.getRegistrationID(), Boolean.FALSE);
			response.getWriter().println(rsp.getMessage());
		}
	}

	private void modifyPerformerAccount(UpdatePerformerAccountRequest rqst,
			HttpServletResponse response) throws IOException {
		try {
//			response.getWriter().println("modifyPerformerAccount");
			// update the performer account in the User table
			Integer accountNumber = this.updateUserTable(rqst.getUsername(),
					rqst.getPassword(), rqst.getEmail());
//			response.getWriter().println("account number = " + accountNumber);
			// retrieve the performer account type from AccountType table
			this.selectAccountTypeFromAccountTypeTable
					.setString(1, "Performer");
			ResultSet accountTypeResult = this.selectAccountTypeFromAccountTypeTable
					.executeQuery();
			Integer accountType = 0;
			while (accountTypeResult.next()) {
				accountType = accountTypeResult.getInt("AccTypeid");
//				 response.getWriter().println(
//				 "<br>Retrieve Performer account type = " + accountType);
			}

			// update the performer account into the Account table
			this.updateAccountTable(accountNumber, accountType,
					rqst.getUsername(), rqst.getDescription(),
					rqst.getWebsite(), rqst.getEvent(), rqst.getImage());
//			response.getWriter().println("finished updating account table");
			// add the performer account into the Performer table
			this.updatePerformerTable.setInt(1, accountNumber);
			this.updatePerformerTable.setInt(2, rqst.getGenre());
			this.updatePerformerTable.setInt(3, accountNumber);
			Integer result = this.updatePerformerTable.executeUpdate();
			if (result != 1) {
				throw new Exception("Failed to update performer table");
			}
			// response.getWriter()
			// .println("<br>Add Performer result = " + result);
			Response rsp = new Response(rqst.getRegistrationID(), Boolean.TRUE);
			response.getWriter().println(rsp.getMessage());
		} catch (Exception e) {
			Response rsp = new Response(rqst.getRegistrationID(), Boolean.FALSE);
			response.getWriter().println(rsp.getMessage());
		}
	}

	private Integer insertIntoUserTable(String username, String password,
			String email) throws SQLException, IOException {
		this.insertIntoUserTable.setString(1, username);
		this.insertIntoUserTable.setString(2, password);
		this.insertIntoUserTable.setString(3, email);
		Integer result = this.insertIntoUserTable.executeUpdate();
		// response.getWriter().println("<br>result from adding user: " +
		// result);
		if (result != 1) {
			throw new IOException("Failed to insert into user table");
		}
		// response.getWriter().println("<br>Add User result = " + result);

		// retrieve the account number to add to account table
		this.selectAccountNumberFromUserTable.setString(1, username);
		ResultSet accountNumberResult = this.selectAccountNumberFromUserTable
				.executeQuery();
		Integer accountNumber = 0;
		while (accountNumberResult.next()) {
			accountNumber = accountNumberResult.getInt("UsrAccNumber");
			// response.getWriter().println(
			// "<br>Retrieve account number = " + accountNumber);
		}

		return accountNumber;
	}

	private Integer updateUserTable(String username, String password,
			String email) throws SQLException, IOException {
		this.updateUserTable.setString(1, password);
		this.updateUserTable.setString(2, email);
		this.updateUserTable.setString(3, username);
		Integer result = this.updateUserTable.executeUpdate();
		// response.getWriter().println("<br>result from updating user: " +
		// result);
		if (result != 1) {
			throw new IOException("Failed to update user table");
		}
		// response.getWriter().println("<br>Update User result = " + result);

		// retrieve the account number to add to account table
		this.selectAccountNumberFromUserTable.setString(1, username);
		ResultSet accountNumberResult = this.selectAccountNumberFromUserTable
				.executeQuery();
		Integer accountNumber = 0;
		while (accountNumberResult.next()) {
			accountNumber = accountNumberResult.getInt("UsrAccNumber");
			// response.getWriter().println(
			// "<br>Retrieve account number = " + accountNumber);
		}

		return accountNumber;
	}

	private void insertIntoAccountTable(Integer accountNumber,
			Integer accountType, String name, String description,
			String website, Integer eventID, byte[] image) throws SQLException,
			IOException {
		this.insertIntoAccountTable.setInt(1, accountNumber);
		this.insertIntoAccountTable.setInt(2, accountType);
		this.insertIntoAccountTable.setString(3, name);
		this.insertIntoAccountTable.setString(4, description);
		this.insertIntoAccountTable.setString(5, website);
		this.insertIntoAccountTable.setInt(6, eventID);
		this.insertIntoAccountTable.setByte(7, image[0]);
		int result = this.insertIntoAccountTable.executeUpdate();
		if (result != 1) {
			throw new IOException("Insert into account table failed");
		}
		// response.getWriter().println("<br>Add Account result = " + result);
	}
	
	private void updateAccountTable(Integer accountNumber,
			Integer accountType, String name, String description,
			String website, Integer eventID, byte[] image) throws SQLException,
			IOException {
		this.updateAccountTable.setInt(1, accountType);
		this.updateAccountTable.setString(2, name);
		this.updateAccountTable.setString(3, description);
		this.updateAccountTable.setString(4, website);
		this.updateAccountTable.setInt(5, eventID);
		this.updateAccountTable.setByte(6, image[0]);
		this.updateAccountTable.setInt(7, accountNumber);
		int result = this.updateAccountTable.executeUpdate();
		if (result != 1) {
			throw new IOException("update account table failed");
		}
		// response.getWriter().println("<br>Add Account result = " + result);
	}
}
