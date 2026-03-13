<script>
import { onMount } from 'svelte';
import { api } from '../../lib/api.js';
import { esc } from '../../lib/utils.js';
import { addToast } from '../../stores/toast.js';

let loading = true;
let products = [];
let error = null;

let pmID = '';
let pmName = '';

async function loadProducts() {
    loading = true;
    error = null;

    try {
        const data = await api('/getProductMap', {});
        products = data || [];
    } catch (e) {
        error = e.message;
        addToast('Failed to load products: ' + e.message, 'error');
    } finally {
        loading = false;
    }
}

async function createProductMapping() {
    const id = parseInt(pmID);
    const product_name = pmName.trim();

    if (!id || !product_name) {
        addToast('Fill all fields', 'error');
        return;
    }

    try {
        await api('/createProductMapping', { id, product_name });
        addToast('Product mapping saved', 'success');
        pmID = '';
        pmName = '';
        loadProducts();
    } catch (e) {
        addToast(e.message, 'error');
    }
}

async function deleteProduct(id) {
    if (!confirm('Delete this product mapping?')) return;

    try {
        await api('/deleteProductMapping', { id });
        addToast('Product mapping deleted', 'success');
        loadProducts();
    } catch (e) {
        addToast(e.message, 'error');
    }
}

onMount(() => {
    loadProducts();
});
</script>

<div class="page active">
    <div class="page-header">
        <h1>Product Map</h1>
    </div>

    <div class="card" style="margin-bottom: 20px;">
        <h3 style="font-size: 16px; font-weight: 600; margin-bottom: 16px;">Create/Update Product Mapping</h3>
        <div style="display: grid; grid-template-columns: auto 1fr auto; gap: 12px; align-items: end;">
            <div class="form-group" style="margin-bottom: 0;">
                <label for="pmID">Product ID</label>
                <input type="number" id="pmID" bind:value={pmID} placeholder="e.g., 1" style="width: 120px;" />
            </div>
            <div class="form-group" style="margin-bottom: 0;">
                <label for="pmName">Product Name</label>
                <input type="text" id="pmName" bind:value={pmName} placeholder="e.g., Cola" />
            </div>
            <button class="btn btn-primary" on:click={createProductMapping}>Save</button>
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
    {:else if products.length === 0}
        <div class="card">
            <div class="empty-state">
                <p>No product mappings</p>
            </div>
        </div>
    {:else}
        <div class="card">
            <div class="table-wrap">
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Name</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each products as p}
                            <tr>
                                <td>{p.id}</td>
                                <td>{esc(p.product_name)}</td>
                                <td>
                                    <button class="btn btn-sm btn-danger" on:click={() => deleteProduct(p.id)}>Delete</button>
                                </td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        </div>
    {/if}
</div>
