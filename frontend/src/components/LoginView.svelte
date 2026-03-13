<script>
    import { setKey } from "../lib/api.js";
    import { addToast } from "../stores/toast.js";

    export let oidcAvailable = false;
    export let checkAuth;

    let apiKey = "";

    function handleAPIKeyLogin() {
        const key = apiKey.trim();
        if (!key) {
            addToast("Please enter an API key", "error");
            return;
        }
        setKey(key);
        checkAuth();
    }

    function handleSSOLogin() {
        window.location.href = "/auth/login";
    }

    function handleKeyPress(e) {
        if (e.key === "Enter") {
            handleAPIKeyLogin();
        }
    }
</script>

<div
    id="loginView"
    class="login-view"
    style="width: 100%; min-height: 100vh; display: flex; justify-content: center; align-items: center;"
>
    <div class="login-container">
        <div class="login-header">
            <h1>Cashless Server</h1>
            <p>Please log in to continue</p>
        </div>
        <div class="login-methods">
            {#if oidcAvailable}
                <button
                    class="btn btn-primary btn-large"
                    on:click={handleSSOLogin}
                >
                    <svg
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        stroke-width="2"
                        style="width: 20px; height: 20px;"
                    >
                        <path
                            d="M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4m-5-4l5-5-5-5m5 5H3"
                        />
                    </svg>
                    Login with SSO
                </button>
                <div class="login-divider">or</div>
            {/if}
            <div class="api-key-login">
                <input
                    type="password"
                    bind:value={apiKey}
                    on:keypress={handleKeyPress}
                    placeholder="Enter API Key"
                />
                <button class="btn btn-primary" on:click={handleAPIKeyLogin}>
                    Login with API Key
                </button>
            </div>
        </div>
    </div>
</div>

<style>
    /* Styles are inherited from styles.css */
    .login-view {
        width: 100%;
        min-height: 100vh;
        display: flex;
        justify-content: center;
        align-items: center;
        background: #0f172a;
    }

    .login-container {
        background: var(--card, #ffffff);
        border-radius: 12px;
        padding: 40px;
        max-width: 420px;
        width: 90%;
        box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
    }

    .login-header {
        text-align: center;
        margin-bottom: 32px;
    }

    .login-header h1 {
        font-size: 28px;
        font-weight: 700;
        margin-bottom: 8px;
        color: var(--text, #1a1a2e);
    }

    .login-header p {
        font-size: 14px;
        color: var(--text-muted, #6b7280);
    }

    .login-methods {
        display: flex;
        flex-direction: column;
        gap: 20px;
    }
</style>
