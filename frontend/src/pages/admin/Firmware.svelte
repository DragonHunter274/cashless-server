<script>
import { onMount } from 'svelte';
import { api, apiUpload } from '../../lib/api.js';
import { fmtDate, fmtBytes, esc } from '../../lib/utils.js';
import { addToast } from '../../stores/toast.js';

let loading = true;
let firmwareList = [];
let error = null;

let version = '';
let fileInput;
let uploading = false;

async function loadFirmware() {
    loading = true;
    error = null;

    try {
        const data = await api('/getFirmwareList', {});
        firmwareList = data || [];
    } catch (e) {
        error = e.message;
        addToast('Failed to load firmware list: ' + e.message, 'error');
    } finally {
        loading = false;
    }
}

async function uploadFirmware() {
    const v = version.trim();
    const file = fileInput?.files?.[0];

    if (!v || !file) {
        addToast('Provide a version and a signed firmware.img file', 'error');
        return;
    }

    const formData = new FormData();
    formData.append('version', v);
    formData.append('firmware', file);

    uploading = true;
    try {
        await apiUpload('/uploadFirmware', formData);
        addToast(`Firmware ${v} uploaded`, 'success');
        version = '';
        fileInput.value = '';
        loadFirmware();
    } catch (e) {
        addToast(e.message, 'error');
    } finally {
        uploading = false;
    }
}

async function activateFirmware(v) {
    if (!confirm(`Activate firmware ${v}? Devices will start updating to this version.`)) return;

    try {
        await api('/activateFirmware', { version: v });
        addToast(`Firmware ${v} activated`, 'success');
        loadFirmware();
    } catch (e) {
        addToast(e.message, 'error');
    }
}

async function deleteFirmware(v) {
    if (!confirm(`Delete firmware ${v}? This cannot be undone.`)) return;

    try {
        await api('/deleteFirmware', { version: v });
        addToast(`Firmware ${v} deleted`, 'success');
        loadFirmware();
    } catch (e) {
        addToast(e.message, 'error');
    }
}

onMount(() => {
    loadFirmware();
});
</script>

<div class="page active">
    <div class="page-header">
        <h1>Firmware</h1>
    </div>

    <div class="card" style="margin-bottom: 20px;">
        <h3 style="font-size: 16px; font-weight: 600; margin-bottom: 16px;">Upload Signed Firmware</h3>
        <p style="margin-bottom: 12px; font-size: 13px; color: var(--text-muted, #6b7280);">
            Upload a firmware.img already signed with your RSA private key (see sign_firmware.sh). The server never
            sees the private key - upload only produces a new inactive version. Activate it separately to publish
            it to devices.
        </p>
        <div style="display: grid; grid-template-columns: auto 1fr auto; gap: 12px; align-items: end;">
            <div class="form-group" style="margin-bottom: 0;">
                <label for="fwVersion">Version</label>
                <input type="text" id="fwVersion" bind:value={version} placeholder="e.g., 1.0.1" style="width: 140px;" />
            </div>
            <div class="form-group" style="margin-bottom: 0;">
                <label for="fwFile">Signed firmware.img</label>
                <input type="file" id="fwFile" bind:this={fileInput} accept=".img,.bin" />
            </div>
            <button class="btn btn-primary" on:click={uploadFirmware} disabled={uploading}>
                {uploading ? 'Uploading...' : 'Upload'}
            </button>
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
    {:else if firmwareList.length === 0}
        <div class="card">
            <div class="empty-state">
                <p>No firmware uploaded yet</p>
            </div>
        </div>
    {:else}
        <div class="card">
            <div class="table-wrap">
                <table>
                    <thead>
                        <tr>
                            <th>Version</th>
                            <th>Filename</th>
                            <th>Size</th>
                            <th>Status</th>
                            <th>Uploaded</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each firmwareList as f}
                            <tr>
                                <td><code>{esc(f.version)}</code></td>
                                <td>{esc(f.filename)}</td>
                                <td>{fmtBytes(f.size)}</td>
                                <td>
                                    {#if f.active}
                                        <span class="badge badge-yes">Active</span>
                                    {:else}
                                        <span class="badge badge-available">Uploaded</span>
                                    {/if}
                                </td>
                                <td>{fmtDate(f.created_at)}</td>
                                <td style="display: flex; gap: 8px;">
                                    {#if !f.active}
                                        <button class="btn btn-sm btn-primary" on:click={() => activateFirmware(f.version)}>Activate</button>
                                        <button class="btn btn-sm btn-danger" on:click={() => deleteFirmware(f.version)}>Delete</button>
                                    {/if}
                                </td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        </div>
    {/if}
</div>
