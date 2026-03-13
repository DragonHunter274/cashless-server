<script>
import { onMount } from 'svelte';
import { api } from '../../lib/api.js';
import { getBadgeClass, esc } from '../../lib/utils.js';
import { addToast } from '../../stores/toast.js';

let loading = true;
let privileges = [];
let error = null;
let uidFilter = '';

let cprvUID = '';
let cprvMachine = '';
let cprvFreeVend = false;

async function loadPrivileges() {
    loading = true;
    error = null;

    try {
        const uid = uidFilter.trim();
        const data = await api('/getPrivileges', uid ? { uid } : {});
        privileges = data || [];
    } catch (e) {
        error = e.message;
        addToast('Failed to load privileges: ' + e.message, 'error');
    } finally {
        loading = false;
    }
}

async function createPrivilegeAction() {
    const uid = cprvUID.trim();
    const machine_id = cprvMachine.trim();

    if (!uid || !machine_id) {
        addToast('Fill all fields', 'error');
        return;
    }

    try {
        await api('/createPrivilege', { uid, machine_id, free_vend: cprvFreeVend });
        addToast('Privilege set', 'success');
        cprvUID = '';
        cprvMachine = '';
        cprvFreeVend = false;
        loadPrivileges();
    } catch (e) {
        addToast(e.message, 'error');
    }
}

async function deletePrivilege(uid, machine_id) {
    if (!confirm('Delete this privilege?')) return;

    try {
        await api('/deletePrivilege', { uid, machine_id });
        addToast('Privilege deleted', 'success');
        loadPrivileges();
    } catch (e) {
        addToast(e.message, 'error');
    }
}

onMount(() => {
    loadPrivileges();
});
</script>

<div class="page active">
    <div class="page-header">
        <h1>Privileges</h1>
    </div>

    <div class="card" style="margin-bottom: 20px;">
        <h3 style="font-size: 16px; font-weight: 600; margin-bottom: 16px;">Create Privilege</h3>
        <div style="display: grid; grid-template-columns: 1fr 1fr auto auto; gap: 12px; align-items: end;">
            <div class="form-group" style="margin-bottom: 0;">
                <label for="cprvUID">User ID</label>
                <input type="text" id="cprvUID" bind:value={cprvUID} placeholder="Enter user ID" />
            </div>
            <div class="form-group" style="margin-bottom: 0;">
                <label for="cprvMachine">Machine ID</label>
                <input type="text" id="cprvMachine" bind:value={cprvMachine} placeholder="e.g., VM01" />
            </div>
            <label class="checkbox-label" style="display: flex; align-items: center; gap: 8px; margin-bottom: 0;">
                <input type="checkbox" bind:checked={cprvFreeVend} id="cprvFreeVend" />
                Free Vend
            </label>
            <button class="btn btn-primary" on:click={createPrivilegeAction}>Create</button>
        </div>
    </div>

    <div class="card" style="margin-bottom: 20px;">
        <div class="form-group" style="margin-bottom: 0;">
            <label for="privFilterUID">Filter by User ID</label>
            <input
                type="text"
                id="privFilterUID"
                bind:value={uidFilter}
                on:input={loadPrivileges}
                placeholder="Leave empty for all"
            />
        </div>
    </div>

    {#if loading}
        <div class="card">
            <p>Loading...</p>
        </div>
    {:else if error}
        <div class="card">
            <p style="color: var(--danger);">{error}</p>
        </div>
    {:else if privileges.length === 0}
        <div class="card">
            <div class="empty-state">
                <p>No privileges found</p>
            </div>
        </div>
    {:else}
        <div class="card">
            <div class="table-wrap">
                <table>
                    <thead>
                        <tr>
                            <th>User</th>
                            <th>Machine</th>
                            <th>Free Vend</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each privileges as p}
                            <tr>
                                <td>{esc(p.uid)}</td>
                                <td>{esc(p.machine_id)}</td>
                                <td><span class={getBadgeClass(p.free_vend ? 'yes' : 'no')}>{p.free_vend ? 'yes' : 'no'}</span></td>
                                <td>
                                    <button class="btn btn-sm btn-danger" on:click={() => deletePrivilege(p.uid, p.machine_id)}>Delete</button>
                                </td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        </div>
    {/if}
</div>
