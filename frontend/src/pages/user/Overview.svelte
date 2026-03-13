<script>
import { onMount } from 'svelte';
import { authState } from '../../stores/auth.js';
import { api } from '../../lib/api.js';
import { fmt, fmtDate, formatAmount, getBadgeClass, esc } from '../../lib/utils.js';
import { addToast } from '../../stores/toast.js';

let loading = true;
let balance = 0;
let transactions = [];
let activeVouchers = 0;
let freeVendMachines = 0;
let error = null;

async function loadData() {
    const uid = $authState?.username;
    if (!uid) return;

    loading = true;
    error = null;

    try {
        const [balanceRes, txsRes, vouchersRes, privilegesRes] = await Promise.all([
            api('/getBalance', { uid }),
            api('/getTransactions', { uid, limit: 10 }),
            api('/getVouchers', { uid }),
            api('/getPrivileges', { uid })
        ]);

        balance = balanceRes.balance;
        transactions = txsRes || [];

        const voucherList = vouchersRes || [];
        const privList = privilegesRes || [];

        activeVouchers = voucherList.filter(v => !v.used).length;
        freeVendMachines = privList.filter(p => p.free_vend).length;
    } catch (e) {
        error = e.message;
        addToast('Failed to load overview: ' + e.message, 'error');
    } finally {
        loading = false;
    }
}

onMount(() => {
    loadData();
});

$: balanceFormatted = formatAmount(balance);
</script>

<div class="page active">
    <div class="page-header">
        <h1>My Overview</h1>
    </div>

    {#if loading}
        <p>Loading...</p>
    {:else if error}
        <div class="card">
            <p style="color: var(--danger);">{error}</p>
        </div>
    {:else}
        <div class="card">
            <h3 style="font-size: 15px; font-weight: 600; margin-bottom: 8px;">Current Balance</h3>
            <div style="font-size: 32px; font-weight: 700;">
                <span class={balanceFormatted.class}>{balanceFormatted.text}</span>
            </div>
        </div>

        <div class="card" style="margin-top: 20px;">
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 16px;">
                <div>
                    <div style="font-size: 11px; color: var(--text-muted); text-transform: uppercase; margin-bottom: 4px;">Active Vouchers</div>
                    <div style="font-size: 20px; font-weight: 600;">{activeVouchers}</div>
                </div>
                <div>
                    <div style="font-size: 11px; color: var(--text-muted); text-transform: uppercase; margin-bottom: 4px;">Free Vend Machines</div>
                    <div style="font-size: 20px; font-weight: 600;">{freeVendMachines}</div>
                </div>
                <div>
                    <div style="font-size: 11px; color: var(--text-muted); text-transform: uppercase; margin-bottom: 4px;">Recent Transactions</div>
                    <div style="font-size: 20px; font-weight: 600;">{transactions.length}</div>
                </div>
            </div>
        </div>

        <div class="card" style="margin-top: 20px;">
            <h4 style="font-size: 14px; font-weight: 600; margin-bottom: 12px;">Recent Activity</h4>
            {#if transactions.length === 0}
                <p style="color: var(--text-muted); font-size: 13px;">No recent transactions</p>
            {:else}
                <div class="table-wrap">
                    <table style="font-size: 12px;">
                        <thead>
                            <tr>
                                <th>Date</th>
                                <th>Amount</th>
                                <th>Product</th>
                                <th>Status</th>
                            </tr>
                        </thead>
                        <tbody>
                            {#each transactions as tx}
                                {@const amtFormatted = formatAmount(tx.amount)}
                                <tr>
                                    <td>{fmtDate(tx.created_at)}</td>
                                    <td><span class={amtFormatted.class}>{amtFormatted.text}</span></td>
                                    <td>{esc(tx.product)}</td>
                                    <td><span class={getBadgeClass(tx.status)}>{tx.status}</span></td>
                                </tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
            {/if}
        </div>
    {/if}
</div>
