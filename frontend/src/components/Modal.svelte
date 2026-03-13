<script>
export let open = false;
export let title = '';
export let onClose = () => {};

function handleClose() {
    open = false;
    onClose();
}

function handleOverlayClick(e) {
    // Close modal if clicking on overlay (not the modal content)
    if (e.target === e.currentTarget) {
        handleClose();
    }
}
</script>

<div class="modal-overlay" class:active={open} on:click={handleOverlayClick} role="presentation">
    <div class="modal">
        <h3>{title}</h3>
        <slot />
        <div class="modal-actions">
            <slot name="actions">
                <button class="btn btn-secondary" on:click={handleClose}>Cancel</button>
            </slot>
        </div>
    </div>
</div>

<style>
/* Styles are inherited from styles.css */
</style>
