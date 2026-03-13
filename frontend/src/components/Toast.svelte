<script>
import toasts from '../stores/toast.js';
</script>

<div id="toasts">
    {#each $toasts as toast (toast.id)}
        <div class="toast toast-{toast.type}">
            {toast.message}
        </div>
    {/each}
</div>

<style>
#toasts {
    position: fixed;
    top: 20px;
    right: 20px;
    z-index: 1000;
    display: flex;
    flex-direction: column;
    gap: 12px;
    max-width: 400px;
    pointer-events: none;
}

.toast {
    background: #ffffff;
    color: #1a1a2e;
    padding: 16px 20px;
    border-radius: 8px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    font-size: 14px;
    font-weight: 500;
    animation: slideIn 0.3s ease-out;
    border-left: 4px solid #3b82f6;
    pointer-events: auto;
    min-width: 250px;
}

@keyframes slideIn {
    from {
        transform: translateX(400px);
        opacity: 0;
    }
    to {
        transform: translateX(0);
        opacity: 1;
    }
}

.toast-success {
    border-left-color: #10b981;
    background: #f0fdf4;
    color: #166534;
}

.toast-error {
    border-left-color: #ef4444;
    background: #fef2f2;
    color: #991b1b;
}

.toast-info {
    border-left-color: #3b82f6;
    background: #eff6ff;
    color: #1e40af;
}
</style>
