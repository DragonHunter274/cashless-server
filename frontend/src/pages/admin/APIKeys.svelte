<script>
import { onMount } from 'svelte';
import { api } from '../../lib/api.js';
import { fmtDate, esc } from '../../lib/utils.js';
import { addToast } from '../../stores/toast.js';

let loading = true;
let apiKeys = [];
let error = null;

let showCreateKeyModal = false;
let showDisplayKeyModal = false;
let newKeyValue = '';
let selectedEndpoints = [];

const ALL_ENDPOINTS = [
    '/getStats', '/getUsers', '/getBalance', '/getTransactions',
    '/getVouchers', '/getPrivileges', '/getProductMap', '/getAPIKeys',
    '/makePurchase', '/confirmPurchase', '/makeCashPurchase', '/topUp',
    '/createUser', '/createVoucher', '/createPrivilege',
    '/createAPIKey', '/deleteAPIKey', '/createProductMapping',
    '/deleteProductMapping', '/deleteVoucher', '/deletePrivilege',
    '/makeRevalue', '/editTransaction', '/deleteTransaction',
    '/uploadFirmware', '/getFirmwareList', '/activateFirmware',
    '/deleteFirmware', '/firmware/firmware.img'
];

async function loadAPIKeys() {
    loading = true;
    error = null;

    try {
        const keys = await api('/getAPIKeys', {}) || [];
        apiKeys = keys;
    } catch (e) {
        error = e.message;
        addToast('Failed to load API keys: ' + e.message, 'error');
    } finally {
        loading = false;
    }
}

function openCreateKeyModal() {
    showCreateKeyModal = true;
    selectedEndpoints = [...ALL_ENDPOINTS]; // Select all by default
}

function closeCreateKeyModal() {
    showCreateKeyModal = false;
    selectedEndpoints = [];
}

function closeDisplayKeyModal() {
    showDisplayKeyModal = false;
    newKeyValue = '';
}

async function createAPIKeyAction() {
    if (selectedEndpoints.length === 0) {
        addToast('Select at least one endpoint', 'error');
        return;
    }

    try {
        const res = await api('/createAPIKey', { allowed_endpoints: selectedEndpoints.join(',') });
        newKeyValue = res.key;
        closeCreateKeyModal();
        showDisplayKeyModal = true;
        loadAPIKeys();
    } catch (e) {
        addToast(e.message, 'error');
    }
}

function copyKey() {
    navigator.clipboard.writeText(newKeyValue).then(() => {
        addToast('Key copied to clipboard', 'success');
    });
}

async function deleteAPIKey(maskedKey) {
    addToast('Cannot delete API keys by masked key. Use the full key.', 'error');
}

function toggleEndpoint(endpoint) {
    if (selectedEndpoints.includes(endpoint)) {
        selectedEndpoints = selectedEndpoints.filter(e => e !== endpoint);
    } else {
        selectedEndpoints = [...selectedEndpoints, endpoint];
    }
}

onMount(() => {
    loadAPIKeys();
});
</script>

<div class="page active">
    <div class="page-header">
        <h1>API Keys</h1>
        <button class="btn btn-primary" on:click={openCreateKeyModal}>Create API Key</button>
    </div>

    {#if loading}
        <div class="card">
            <p>Loading...</p>
        </div>
    {:else if error}
        <div class="card">
            <p style="color: var(--danger);">{error}</p>
        </div>
    {:else if apiKeys.length === 0}
        <div class="card">
            <div class="empty-state">
                <p>No API keys</p>
            </div>
        </div>
    {:else}
        <div class="card">
            <div class="table-wrap">
                <table>
                    <thead>
                        <tr>
                            <th>Key</th>
                            <th>Endpoints</th>
                            <th>Created</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each apiKeys as k}
                            <tr>
                                <td><code>{esc(k.key)}</code></td>
                                <td style="max-width:300px;word-break:break-all;font-size:11px;">{esc(k.allowed_endpoints)}</td>
                                <td>{fmtDate(k.created_at)}</td>
                                <td>
                                    <button class="btn btn-sm btn-danger" on:click={() => deleteAPIKey(k.key)}>Delete</button>
                                </td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        </div>
    {/if}
</div>

<!-- Create API Key Modal -->
{#if showCreateKeyModal}
    <div class="modal-overlay" on:click={closeCreateKeyModal} role="button" tabindex="-1" on:keydown={(e) => e.key === 'Escape' && closeCreateKeyModal()}>
        <div class="modal" on:click|stopPropagation on:keydown role="dialog" aria-modal="true" tabindex="-1" style="max-width: 600px;">
            <div class="modal-header">
                <h3>Create API Key</h3>
                <button class="close-btn" on:click={closeCreateKeyModal} aria-label="Close">&times;</button>
            </div>
            <div class="modal-body">
                <p style="margin-bottom: 12px; font-size: 14px;">Select the endpoints this key can access:</p>
                <div id="endpointCheckboxes" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 8px; max-height: 400px; overflow-y: auto;">
                    {#each ALL_ENDPOINTS as ep}
                        <label class="checkbox-label" style="display: flex; align-items: center; gap: 8px;">
                            <input
                                type="checkbox"
                                checked={selectedEndpoints.includes(ep)}
                                on:change={() => toggleEndpoint(ep)}
                            />
                            {ep}
                        </label>
                    {/each}
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" on:click={closeCreateKeyModal}>Cancel</button>
                <button class="btn btn-primary" on:click={createAPIKeyAction}>Create</button>
            </div>
        </div>
    </div>
{/if}

<!-- Display New Key Modal -->
{#if showDisplayKeyModal}
    <div class="modal-overlay" on:click={closeDisplayKeyModal} role="button" tabindex="-1" on:keydown={(e) => e.key === 'Escape' && closeDisplayKeyModal()}>
        <div class="modal" on:click|stopPropagation on:keydown role="dialog" aria-modal="true" tabindex="-1">
            <div class="modal-header">
                <h3>API Key Created</h3>
                <button class="close-btn" on:click={closeDisplayKeyModal} aria-label="Close">&times;</button>
            </div>
            <div class="modal-body">
                <p style="margin-bottom: 12px; font-size: 14px;">Your new API key has been created. Copy it now - you won't be able to see it again!</p>
                <div style="background: var(--surface); padding: 12px; border-radius: 6px; border: 1px solid var(--border); word-break: break-all; font-family: monospace; font-size: 12px; margin-bottom: 12px;">
                    {newKeyValue}
                </div>
                <button class="btn btn-primary" on:click={copyKey}>Copy to Clipboard</button>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" on:click={closeDisplayKeyModal}>Close</button>
            </div>
        </div>
    </div>
{/if}
