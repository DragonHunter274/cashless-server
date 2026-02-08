// Page-specific handlers and data loading functions
// Dependencies: utils.js, app.js (for api function and state variables)

// ===== DASHBOARD =====
async function loadDashboard() {
    try {
        const s = await api('/getStats', {});
        document.getElementById('statsGrid').innerHTML = `
            <div class="stat-card"><div class="stat-label">Total Users</div><div class="stat-value">${s.total_users}</div></div>
            <div class="stat-card"><div class="stat-label">Revenue</div><div class="stat-value revenue">${fmt(-s.total_revenue)}</div></div>
            <div class="stat-card"><div class="stat-label">Confirmed Txs</div><div class="stat-value">${s.confirmed_transactions}</div></div>
            <div class="stat-card"><div class="stat-label">Pending Txs</div><div class="stat-value pending">${s.pending_transactions}</div></div>
            <div class="stat-card"><div class="stat-label">Failed Txs</div><div class="stat-value failed">${s.failed_transactions}</div></div>
            <div class="stat-card"><div class="stat-label">Active Vouchers</div><div class="stat-value">${s.active_vouchers}</div></div>
            <div class="stat-card"><div class="stat-label">Used Vouchers</div><div class="stat-value">${s.used_vouchers}</div></div>
            <div class="stat-card"><div class="stat-label">Privileges</div><div class="stat-value">${s.total_privileges}</div></div>
        `;

        const txs = s.recent_transactions || [];
        if (txs.length === 0) {
            document.getElementById('recentActivity').innerHTML = '<div class="empty-state"><p>No transactions yet</p></div>';
        } else {
            document.getElementById('recentActivity').innerHTML = `<table>
                <thead><tr><th>ID</th><th>User</th><th>Amount</th><th>Product</th><th>Status</th><th>Date</th></tr></thead>
                <tbody>${txs.map(tx => `<tr>
                    <td>${tx.transaction_id}</td>
                    <td>${tx.uid || 'N/A'}</td>
                    <td>${amountHtml(tx.amount)}</td>
                    <td>${esc(tx.product)}</td>
                    <td>${badge(tx.status)}</td>
                    <td>${fmtDate(tx.created_at)}</td>
                </tr>`).join('')}</tbody></table>`;
        }
    } catch (e) {
        document.getElementById('statsGrid').innerHTML = `<div class="stat-card"><div class="stat-label">Error</div><div class="stat-value" style="font-size:14px;">${esc(e.message)}</div></div>`;
    }
}

// ===== USERS =====
async function loadUsers() {
    try {
        const search = document.getElementById('userSearch').value;
        const data = await api('/getUsers', { search, limit: 25, offset: window.usersOffset });
        const users = data.users || [];

        if (users.length === 0) {
            document.getElementById('usersTable').innerHTML = '<div class="empty-state"><p>No users found</p></div>';
            document.getElementById('usersPagination').innerHTML = '';
            return;
        }

        document.getElementById('usersTable').innerHTML = `<table>
            <thead><tr><th>UID</th><th>Balance</th><th>Created</th></tr></thead>
            <tbody>${users.map(u => `<tr class="clickable-row" onclick="showUserDetail('${esc(u.uid)}')">
                <td><strong>${esc(u.uid)}</strong></td>
                <td>${amountHtml(u.balance)}</td>
                <td>${fmtDate(u.created_at)}</td>
            </tr>`).join('')}</tbody></table>`;

        const totalPages = Math.ceil(data.total / 25);
        const currentPg = Math.floor(window.usersOffset / 25) + 1;
        document.getElementById('usersPagination').innerHTML = `
            <button class="btn btn-sm btn-secondary" ${window.usersOffset === 0 ? 'disabled' : ''} onclick="window.usersOffset-=25;loadUsers()">Prev</button>
            <span class="page-info">Page ${currentPg} of ${totalPages} (${data.total} users)</span>
            <button class="btn btn-sm btn-secondary" ${currentPg >= totalPages ? 'disabled' : ''} onclick="window.usersOffset+=25;loadUsers()">Next</button>
        `;
    } catch (e) {
        document.getElementById('usersTable').innerHTML = `<div class="empty-state"><p>Error: ${esc(e.message)}</p></div>`;
    }
}

async function showUserDetail(uid) {
    const el = document.getElementById('userDetail');
    el.innerHTML = '<div class="detail-panel"><p>Loading...</p></div>';

    try {
        const [balance, txs, vouchers, privileges] = await Promise.all([
            api('/getBalance', { uid }),
            api('/getTransactions', { uid, limit: 20 }),
            api('/getVouchers', { uid }),
            api('/getPrivileges', { uid })
        ]);

        const bal = balance.balance;
        const transactions = txs || [];
        const voucherList = vouchers || [];
        const privList = privileges || [];

        el.innerHTML = `<div class="detail-panel">
            <div class="detail-header">
                <h3>${esc(uid)}</h3>
                <div>
                    <button class="btn btn-sm btn-primary" onclick="showTopUpModal('${esc(uid)}')">Top Up</button>
                    <button class="btn btn-sm btn-secondary" onclick="document.getElementById('userDetail').innerHTML=''">Close</button>
                </div>
            </div>
            <div class="detail-balance ${bal >= 0 ? 'amount-positive' : 'amount-negative'}">${fmt(bal)}</div>

            <div class="detail-section">
                <h4>Vouchers (${voucherList.length})</h4>
                ${voucherList.length === 0 ? '<p style="color:var(--text-muted);font-size:13px;">None</p>' :
                `<table><thead><tr><th>ID</th><th>Machine</th><th>Status</th><th>Created</th></tr></thead>
                <tbody>${voucherList.map(v => `<tr>
                    <td>${v.id}</td>
                    <td>${esc(v.machine_id)}</td>
                    <td>${v.used ? badge('used') : badge('available')}</td>
                    <td>${fmtDate(v.created_at)}</td>
                </tr>`).join('')}</tbody></table>`}
            </div>

            <div class="detail-section">
                <h4>Privileges (${privList.length})</h4>
                ${privList.length === 0 ? '<p style="color:var(--text-muted);font-size:13px;">None</p>' :
                `<table><thead><tr><th>Machine</th><th>Free Vend</th></tr></thead>
                <tbody>${privList.map(p => `<tr>
                    <td>${esc(p.machine_id)}</td>
                    <td>${p.free_vend ? badge('yes') : badge('no')}</td>
                </tr>`).join('')}</tbody></table>`}
            </div>

            <div class="detail-section">
                <h4>Recent Transactions (${transactions.length})</h4>
                ${transactions.length === 0 ? '<p style="color:var(--text-muted);font-size:13px;">None</p>' :
                `<table><thead><tr><th>Date</th><th>Amount</th><th>Product</th><th>Machine</th><th>Status</th></tr></thead>
                <tbody>${transactions.map(tx => `<tr>
                    <td>${fmtDate(tx.created_at)}</td>
                    <td>${amountHtml(tx.amount)}</td>
                    <td>${esc(tx.product)}</td>
                    <td>${esc(tx.machine_id)}</td>
                    <td>${badge(tx.status)}</td>
                </tr>`).join('')}</tbody></table>`}
            </div>
        </div>`;
    } catch (e) {
        el.innerHTML = `<div class="detail-panel"><p style="color:var(--danger);">Error: ${esc(e.message)}</p></div>`;
    }
}

function showCreateUserModal() {
    showModal('createUserModal');
    document.getElementById('newUserUID').value = '';
    document.getElementById('newUserUID').focus();
}

async function createUserAction() {
    const uid = document.getElementById('newUserUID').value.trim();
    if (!uid) return toast('Enter a UID', 'error');
    try {
        await api('/createUser', { uid });
        toast('User created: ' + uid);
        closeModal('createUserModal');
        loadUsers();
    } catch (e) { toast(e.message, 'error'); }
}

function showTopUpModal(uid) {
    document.getElementById('topUpUID').value = uid;
    document.getElementById('topUpAmount').value = '';
    showModal('topUpModal');
    document.getElementById('topUpAmount').focus();
}

async function topUpAction() {
    const uid = document.getElementById('topUpUID').value;
    const amount = parseInt(document.getElementById('topUpAmount').value);
    if (!amount || amount <= 0) return toast('Enter a valid amount', 'error');
    try {
        await api('/topUp', { uid, amount });
        toast(`Topped up ${uid} with ${fmt(amount)}`);
        closeModal('topUpModal');
        showUserDetail(uid);
        loadUsers();
    } catch (e) { toast(e.message, 'error'); }
}

// ===== PURCHASES =====
async function makeDigitalPurchase() {
    const uid = document.getElementById('dpUID').value.trim();
    const amount = parseInt(document.getElementById('dpAmount').value);
    const product = parseInt(document.getElementById('dpProduct').value);
    const machine_id = document.getElementById('dpMachine').value.trim();
    if (!uid || !amount || !product || !machine_id) return toast('Fill all fields', 'error');
    try {
        const res = await api('/makePurchase', { uid, amount, product, machine_id });
        window.pendingTxs.push({ ...res, uid, amount, product });
        renderPending();
        toast('Purchase created (pending). ID: ' + res.transaction_id);
    } catch (e) { toast(e.message, 'error'); }
}

async function makeCashPurchaseAction() {
    const amount = parseInt(document.getElementById('cpAmount').value);
    const product = parseInt(document.getElementById('cpProduct').value);
    const machine_id = document.getElementById('cpMachine').value.trim();
    if (!amount || !product || !machine_id) return toast('Fill all fields', 'error');
    try {
        const res = await api('/makeCashPurchase', { amount, product, machine_id });
        toast('Cash purchase confirmed. ID: ' + res.transaction_id);
        document.getElementById('cpAmount').value = '';
        document.getElementById('cpProduct').value = '';
    } catch (e) { toast(e.message, 'error'); }
}

function renderPending() {
    const el = document.getElementById('pendingList');
    if (window.pendingTxs.length === 0) {
        el.innerHTML = '<div class="empty-state"><p>No pending purchases</p></div>';
        return;
    }
    el.innerHTML = `<table><thead><tr><th>ID</th><th>User</th><th>Amount</th><th>Product</th><th>Action</th></tr></thead>
        <tbody>${window.pendingTxs.map(tx => `<tr>
            <td>${tx.transaction_id}</td>
            <td>${esc(tx.uid || 'N/A')}</td>
            <td>${fmt(tx.amount)}</td>
            <td>${tx.product}</td>
            <td><button class="btn btn-sm btn-primary" onclick="confirmPurchase(${tx.transaction_id})">Confirm</button></td>
        </tr>`).join('')}</tbody></table>`;
}

async function confirmPurchase(id) {
    try {
        await api('/confirmPurchase', { transaction_id: id });
        window.pendingTxs = window.pendingTxs.filter(tx => tx.transaction_id !== id);
        renderPending();
        toast('Purchase confirmed');
    } catch (e) { toast(e.message, 'error'); }
}

// ===== VOUCHERS =====
async function loadVouchers() {
    try {
        const uid = document.getElementById('voucherFilterUID').value.trim();
        const data = await api('/getVouchers', uid ? { uid } : {});
        const vouchers = data || [];

        if (vouchers.length === 0) {
            document.getElementById('vouchersTable').innerHTML = '<div class="empty-state"><p>No vouchers found</p></div>';
            return;
        }

        document.getElementById('vouchersTable').innerHTML = `<table>
            <thead><tr><th>ID</th><th>User</th><th>Machine</th><th>Status</th><th>Created</th><th>Action</th></tr></thead>
            <tbody>${vouchers.map(v => `<tr>
                <td>${v.id}</td>
                <td>${esc(v.uid)}</td>
                <td>${esc(v.machine_id)}</td>
                <td>${v.used ? badge('used') : badge('available')}</td>
                <td>${fmtDate(v.created_at)}</td>
                <td>${v.used ? '' : `<button class="btn btn-sm btn-danger" onclick="deleteVoucher(${v.id})">Delete</button>`}</td>
            </tr>`).join('')}</tbody></table>`;
    } catch (e) { toast(e.message, 'error'); }
}

async function createVoucherAction() {
    const uid = document.getElementById('cvUID').value.trim();
    const machine_id = document.getElementById('cvMachine').value.trim();
    if (!uid || !machine_id) return toast('Fill all fields', 'error');
    try {
        await api('/createVoucher', { uid, machine_id });
        toast('Voucher created');
        document.getElementById('cvUID').value = '';
        document.getElementById('cvMachine').value = '';
        loadVouchers();
    } catch (e) { toast(e.message, 'error'); }
}

async function deleteVoucher(id) {
    if (!confirm('Delete this voucher?')) return;
    try {
        await api('/deleteVoucher', { id });
        toast('Voucher deleted');
        loadVouchers();
    } catch (e) { toast(e.message, 'error'); }
}

// ===== PRIVILEGES =====
async function loadPrivileges() {
    try {
        const uid = document.getElementById('privFilterUID').value.trim();
        const data = await api('/getPrivileges', uid ? { uid } : {});
        const privs = data || [];

        if (privs.length === 0) {
            document.getElementById('privilegesTable').innerHTML = '<div class="empty-state"><p>No privileges found</p></div>';
            return;
        }

        document.getElementById('privilegesTable').innerHTML = `<table>
            <thead><tr><th>User</th><th>Machine</th><th>Free Vend</th><th>Action</th></tr></thead>
            <tbody>${privs.map(p => `<tr>
                <td>${esc(p.uid)}</td>
                <td>${esc(p.machine_id)}</td>
                <td>${p.free_vend ? badge('yes') : badge('no')}</td>
                <td><button class="btn btn-sm btn-danger" onclick="deletePrivilege('${esc(p.uid)}','${esc(p.machine_id)}')">Delete</button></td>
            </tr>`).join('')}</tbody></table>`;
    } catch (e) { toast(e.message, 'error'); }
}

async function createPrivilegeAction() {
    const uid = document.getElementById('cprvUID').value.trim();
    const machine_id = document.getElementById('cprvMachine').value.trim();
    const free_vend = document.getElementById('cprvFreeVend').checked;
    if (!uid || !machine_id) return toast('Fill all fields', 'error');
    try {
        await api('/createPrivilege', { uid, machine_id, free_vend });
        toast('Privilege set');
        document.getElementById('cprvUID').value = '';
        document.getElementById('cprvMachine').value = '';
        document.getElementById('cprvFreeVend').checked = false;
        loadPrivileges();
    } catch (e) { toast(e.message, 'error'); }
}

async function deletePrivilege(uid, machine_id) {
    if (!confirm('Delete this privilege?')) return;
    try {
        await api('/deletePrivilege', { uid, machine_id });
        toast('Privilege deleted');
        loadPrivileges();
    } catch (e) { toast(e.message, 'error'); }
}

// ===== TRANSACTIONS =====
async function loadTransactions() {
    try {
        const uid = document.getElementById('txFilterUID').value.trim();
        const limit = parseInt(document.getElementById('txFilterLimit').value);
        const body = { limit, offset: window.txOffset };
        if (uid) body.uid = uid;
        const txs = await api('/getTransactions', body) || [];

        if (txs.length === 0) {
            document.getElementById('transactionsTable').innerHTML = '<div class="empty-state"><p>No transactions found</p></div>';
            document.getElementById('txPagination').innerHTML = '';
            return;
        }

        document.getElementById('transactionsTable').innerHTML = `<table>
            <thead><tr><th>ID</th><th>User</th><th>Amount</th><th>Product</th><th>Machine</th><th>Method</th><th>Status</th><th>Date</th></tr></thead>
            <tbody>${txs.map(tx => `<tr>
                <td>${tx.transaction_id}</td>
                <td>${tx.uid ? esc(tx.uid) : 'N/A'}</td>
                <td>${amountHtml(tx.amount)}</td>
                <td>${esc(tx.product)}</td>
                <td>${esc(tx.machine_id)}</td>
                <td>${esc(tx.payment_method)}</td>
                <td>${badge(tx.status)}</td>
                <td>${fmtDate(tx.created_at)}</td>
            </tr>`).join('')}</tbody></table>`;

        document.getElementById('txPagination').innerHTML = `
            <button class="btn btn-sm btn-secondary" ${window.txOffset === 0 ? 'disabled' : ''} onclick="window.txOffset-=${limit};loadTransactions()">Prev</button>
            <span class="page-info">Showing ${window.txOffset + 1}-${window.txOffset + txs.length}</span>
            <button class="btn btn-sm btn-secondary" ${txs.length < limit ? 'disabled' : ''} onclick="window.txOffset+=${limit};loadTransactions()">Next</button>
        `;
    } catch (e) { toast(e.message, 'error'); }
}

// ===== PRODUCTS =====
async function loadProducts() {
    try {
        const data = await api('/getProductMap', {});
        const products = data || [];

        if (products.length === 0) {
            document.getElementById('productsTable').innerHTML = '<div class="empty-state"><p>No product mappings</p></div>';
            return;
        }

        document.getElementById('productsTable').innerHTML = `<table>
            <thead><tr><th>ID</th><th>Name</th><th>Action</th></tr></thead>
            <tbody>${products.map(p => `<tr>
                <td>${p.id}</td>
                <td>${esc(p.product_name)}</td>
                <td><button class="btn btn-sm btn-danger" onclick="deleteProduct(${p.id})">Delete</button></td>
            </tr>`).join('')}</tbody></table>`;
    } catch (e) { toast(e.message, 'error'); }
}

async function createProductMapping() {
    const id = parseInt(document.getElementById('pmID').value);
    const product_name = document.getElementById('pmName').value.trim();
    if (!id || !product_name) return toast('Fill all fields', 'error');
    try {
        await api('/createProductMapping', { id, product_name });
        toast('Product mapping saved');
        document.getElementById('pmID').value = '';
        document.getElementById('pmName').value = '';
        loadProducts();
    } catch (e) { toast(e.message, 'error'); }
}

async function deleteProduct(id) {
    if (!confirm('Delete this product mapping?')) return;
    try {
        await api('/deleteProductMapping', { id });
        toast('Product mapping deleted');
        loadProducts();
    } catch (e) { toast(e.message, 'error'); }
}

// ===== API KEYS =====
const ALL_ENDPOINTS = [
    '/getStats', '/getUsers', '/getBalance', '/getTransactions',
    '/getVouchers', '/getPrivileges', '/getProductMap', '/getAPIKeys',
    '/makePurchase', '/confirmPurchase', '/makeCashPurchase', '/topUp',
    '/createUser', '/createVoucher', '/createPrivilege',
    '/createAPIKey', '/deleteAPIKey', '/createProductMapping',
    '/deleteProductMapping', '/deleteVoucher', '/deletePrivilege'
];

async function loadAPIKeys() {
    try {
        const keys = await api('/getAPIKeys', {}) || [];

        if (keys.length === 0) {
            document.getElementById('apiKeysTable').innerHTML = '<div class="empty-state"><p>No API keys</p></div>';
            return;
        }

        document.getElementById('apiKeysTable').innerHTML = `<table>
            <thead><tr><th>Key</th><th>Endpoints</th><th>Created</th><th>Action</th></tr></thead>
            <tbody>${keys.map(k => `<tr>
                <td><code>${esc(k.key)}</code></td>
                <td style="max-width:300px;word-break:break-all;font-size:11px;">${esc(k.allowed_endpoints)}</td>
                <td>${fmtDate(k.created_at)}</td>
                <td><button class="btn btn-sm btn-danger" onclick="deleteAPIKey('${esc(k.key)}')">Delete</button></td>
            </tr>`).join('')}</tbody></table>`;
    } catch (e) { toast(e.message, 'error'); }
}

function showCreateKeyModal() {
    const grid = document.getElementById('endpointCheckboxes');
    grid.innerHTML = ALL_ENDPOINTS.map(ep =>
        `<label class="checkbox-label"><input type="checkbox" value="${ep}" checked> ${ep}</label>`
    ).join('');
    showModal('createKeyModal');
}

async function createAPIKeyAction() {
    const checked = [...document.querySelectorAll('#endpointCheckboxes input:checked')].map(c => c.value);
    if (checked.length === 0) return toast('Select at least one endpoint', 'error');
    try {
        const res = await api('/createAPIKey', { allowed_endpoints: checked.join(',') });
        closeModal('createKeyModal');
        document.getElementById('newKeyDisplay').textContent = res.key;
        showModal('showKeyModal');
        loadAPIKeys();
    } catch (e) { toast(e.message, 'error'); }
}

function copyKey() {
    const key = document.getElementById('newKeyDisplay').textContent;
    navigator.clipboard.writeText(key).then(() => toast('Key copied to clipboard'));
}

async function deleteAPIKey(maskedKey) {
    toast('Cannot delete API keys by masked key. Use the full key.', 'error');
}

// ===== USER VIEW PAGES =====

// Get current user's UID (use username from auth state)
function getUserUID() {
    return window.authState ? window.authState.username : null;
}

// User Overview Page
async function loadUserOverview() {
    const uid = getUserUID();
    if (!uid) return;

    try {
        const [balance, txs, vouchers, privileges] = await Promise.all([
            api('/getBalance', { uid }),
            api('/getTransactions', { uid, limit: 10 }),
            api('/getVouchers', { uid }),
            api('/getPrivileges', { uid })
        ]);

        const bal = balance.balance;
        const transactions = txs || [];
        const voucherList = vouchers || [];
        const privList = privileges || [];

        // Display balance
        const balClass = bal >= 0 ? 'amount-positive' : 'amount-negative';
        document.getElementById('userBalance').innerHTML = `<span class="${balClass}">${fmt(bal)}</span>`;

        // Display summary
        const activeVouchers = voucherList.filter(v => !v.used).length;
        const freeVendPrivs = privList.filter(p => p.free_vend).length;

        document.getElementById('userSummary').innerHTML = `
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 16px; margin-top: 12px;">
                <div>
                    <div style="font-size: 11px; color: var(--text-muted); text-transform: uppercase; margin-bottom: 4px;">Active Vouchers</div>
                    <div style="font-size: 20px; font-weight: 600;">${activeVouchers}</div>
                </div>
                <div>
                    <div style="font-size: 11px; color: var(--text-muted); text-transform: uppercase; margin-bottom: 4px;">Free Vend Machines</div>
                    <div style="font-size: 20px; font-weight: 600;">${freeVendPrivs}</div>
                </div>
                <div>
                    <div style="font-size: 11px; color: var(--text-muted); text-transform: uppercase; margin-bottom: 4px;">Recent Transactions</div>
                    <div style="font-size: 20px; font-weight: 600;">${transactions.length}</div>
                </div>
            </div>

            <div style="margin-top: 24px;">
                <h4 style="font-size: 14px; font-weight: 600; margin-bottom: 12px;">Recent Activity</h4>
                ${transactions.length === 0 ? '<p style="color: var(--text-muted); font-size: 13px;">No recent transactions</p>' :
                `<table style="font-size: 12px;">
                    <thead><tr><th>Date</th><th>Amount</th><th>Product</th><th>Status</th></tr></thead>
                    <tbody>${transactions.map(tx => `<tr>
                        <td>${fmtDate(tx.created_at)}</td>
                        <td>${amountHtml(tx.amount)}</td>
                        <td>${esc(tx.product)}</td>
                        <td>${badge(tx.status)}</td>
                    </tr>`).join('')}</tbody>
                </table>`}
            </div>
        `;
    } catch (e) {
        document.getElementById('userBalance').innerHTML = '<span style="color: var(--danger);">Error loading data</span>';
        document.getElementById('userSummary').innerHTML = `<p style="color: var(--danger);">${esc(e.message)}</p>`;
    }
}

// User Transactions Page
async function loadUserTransactions() {
    const uid = getUserUID();
    if (!uid) return;

    try {
        const txs = await api('/getTransactions', { uid, limit: 100 }) || [];

        if (txs.length === 0) {
            document.getElementById('userTransactionsTable').innerHTML = '<div class="empty-state"><p>No transactions found</p></div>';
            return;
        }

        document.getElementById('userTransactionsTable').innerHTML = `<table>
            <thead><tr><th>Date</th><th>Amount</th><th>Product</th><th>Machine</th><th>Method</th><th>Status</th></tr></thead>
            <tbody>${txs.map(tx => `<tr>
                <td>${fmtDate(tx.created_at)}</td>
                <td>${amountHtml(tx.amount)}</td>
                <td>${esc(tx.product)}</td>
                <td>${esc(tx.machine_id)}</td>
                <td>${esc(tx.payment_method)}</td>
                <td>${badge(tx.status)}</td>
            </tr>`).join('')}</tbody>
        </table>`;
    } catch (e) {
        document.getElementById('userTransactionsTable').innerHTML = `<div class="empty-state"><p style="color: var(--danger);">Error: ${esc(e.message)}</p></div>`;
    }
}

// User Vouchers Page
async function loadUserVouchers() {
    const uid = getUserUID();
    if (!uid) return;

    try {
        const vouchers = await api('/getVouchers', { uid }) || [];

        if (vouchers.length === 0) {
            document.getElementById('userVouchersTable').innerHTML = '<div class="empty-state"><p>No vouchers found</p></div>';
            return;
        }

        document.getElementById('userVouchersTable').innerHTML = `<table>
            <thead><tr><th>ID</th><th>Machine</th><th>Status</th><th>Created</th></tr></thead>
            <tbody>${vouchers.map(v => `<tr>
                <td>${v.id}</td>
                <td>${esc(v.machine_id)}</td>
                <td>${v.used ? badge('used') : badge('available')}</td>
                <td>${fmtDate(v.created_at)}</td>
            </tr>`).join('')}</tbody>
        </table>`;
    } catch (e) {
        document.getElementById('userVouchersTable').innerHTML = `<div class="empty-state"><p style="color: var(--danger);">Error: ${esc(e.message)}</p></div>`;
    }
}

// User Privileges Page
async function loadUserPrivileges() {
    const uid = getUserUID();
    if (!uid) return;

    try {
        const privs = await api('/getPrivileges', { uid }) || [];

        if (privs.length === 0) {
            document.getElementById('userPrivilegesTable').innerHTML = '<div class="empty-state"><p>No privileges found</p></div>';
            return;
        }

        document.getElementById('userPrivilegesTable').innerHTML = `<table>
            <thead><tr><th>Machine</th><th>Free Vend</th></tr></thead>
            <tbody>${privs.map(p => `<tr>
                <td>${esc(p.machine_id)}</td>
                <td>${p.free_vend ? badge('yes') : badge('no')}</td>
            </tr>`).join('')}</tbody>
        </table>`;
    } catch (e) {
        document.getElementById('userPrivilegesTable').innerHTML = `<div class="empty-state"><p style="color: var(--danger);">Error: ${esc(e.message)}</p></div>`;
    }
}
