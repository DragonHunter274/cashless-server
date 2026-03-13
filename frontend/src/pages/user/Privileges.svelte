<script>
import { onMount } from 'svelte';
import { authState } from '../../stores/auth.js';
import { api } from '../../lib/api.js';
import { getBadgeClass, esc } from '../../lib/utils.js';
import { addToast } from '../../stores/toast.js';

let loading = true;
let privileges = [];
let error = null;

async function loadData() {
    const uid = $authState?.username;
    if (!uid) return;

    loading = true;
    error = null;

    try {
        privileges = await api('/getPrivileges', { uid }) || [];
    } catch (e) {
        error = e.message;
        addToast('Failed to load privileges: ' + e.message, 'error');
    } finally {
        loading = false;
    }
}

onMount(() => {
    loadData();
});
</script>

<div class="page active">
    <div class="page-header">
        <h1>My Privileges</h1>
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
                            <th>Machine</th>
                            <th>Free Vend</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each privileges as p}
                            <tr>
                                <td>{esc(p.machine_id)}</td>
                                <td><span class={getBadgeClass(p.free_vend ? 'yes' : 'no')}>{p.free_vend ? 'yes' : 'no'}</span></td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        </div>
    {/if}
</div>
