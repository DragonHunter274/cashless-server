# Cashless Server Web Frontend

A modern, single-page web interface for managing the cashless payment server.

## Features

### User Management
- **Create Users**: Register new users with their UID
- **Check Balance**: View current balance for any user
- **Top Up**: Add funds to user accounts

### Purchase Management
- **Digital Purchases**: Create pending purchases that require confirmation
- **Confirm Purchases**: Confirm pending transactions (simulates successful vend)
- **Cash Purchases**: Record anonymous cash transactions

### Voucher Management
- **Create Vouchers**: Issue single-use vouchers for specific users and machines

### Privilege Management
- **Set Machine Privileges**: Grant free vend privileges to users on specific machines

### Transaction History
- View transaction history (note: endpoint needs to be implemented in the API)

## Usage

### Quick Start (Test Mode)

The easiest way to try the web UI:

1. **Start the server in test mode**:
   ```bash
   ./cashless-server -test
   ```

2. **Copy the API key** displayed in the terminal

3. **Access the frontend**:
   Open your browser and navigate to `http://localhost:8080`

4. **Paste the API Key**:
   - Paste the API key into the input field at the top of the page
   - Start managing your cashless system!

### Production Mode

1. **Start the server**:
   ```bash
   # Make sure PostgreSQL environment variables are set
   export PG_USER=your_user
   export PG_PASSWORD=your_password
   export PG_DBNAME=your_database
   export PG_HOST=localhost:5432

   # Run the server
   ./cashless-server
   ```

2. **Access the frontend**:
   Open your browser and navigate to `http://localhost:8080`

3. **Configure API Key**:
   - Enter your API key in the header section
   - The API key is stored in the browser session only
   - Get an API key from your database's `api_keys` table

## Design Features

- **Responsive Design**: Works on desktop, tablet, and mobile devices
- **Modern UI**: Clean gradient design with smooth animations
- **Real-time Feedback**: Success/error messages for all operations
- **Pending Purchase Management**: Track and confirm pending digital purchases
- **Tab Navigation**: Organized into logical sections

## Security Notes

- All API endpoints require authentication via `X-API-Key` header
- API keys are not stored persistently by the frontend
- Ensure you're using HTTPS in production
- The `/metrics` endpoint is the only unauthenticated endpoint

## Browser Compatibility

Works with all modern browsers:
- Chrome/Edge (latest)
- Firefox (latest)
- Safari (latest)

## Future Enhancements

- Add transaction history endpoint to the API
- Add filtering and search capabilities
- Add export functionality for reports
- Add real-time updates via WebSocket
- Add product mapping management
- Add API key management interface
