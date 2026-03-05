// Activity Stack & Match - Frontend JS
(function () {
    const FILE_KEYS = [
        "prev_week", "bank_statements", "all_transactions", "loan_report",
        "search_strings", "static_mapping"
    ];
    const files = {};
    let sessionId = null;
    let running = false;

    // DOM references
    const runBtn = document.getElementById("run-btn");
    const downloadBtn = document.getElementById("download-btn");
    const logPanel = document.getElementById("log-panel");
    const statsPanel = document.getElementById("stats-panel");
    const errorBox = document.getElementById("error-box");

    // Set up drop zones
    FILE_KEYS.forEach(key => {
        const zone = document.getElementById("drop-" + key);
        if (!zone) return;
        const input = zone.querySelector("input[type=file]");

        zone.addEventListener("click", () => input.click());
        zone.addEventListener("dragover", e => {
            e.preventDefault();
            zone.classList.add("dragging");
        });
        zone.addEventListener("dragleave", () => zone.classList.remove("dragging"));
        zone.addEventListener("drop", e => {
            e.preventDefault();
            zone.classList.remove("dragging");
            if (e.dataTransfer.files.length > 0) {
                handleFile(key, e.dataTransfer.files[0], zone);
            }
        });
        input.addEventListener("change", () => {
            if (input.files.length > 0) {
                handleFile(key, input.files[0], zone);
            }
        });
    });

    function handleFile(key, file, zone) {
        files[key] = file;
        zone.classList.add("has-file");
        zone.querySelector(".drop-filename").textContent = file.name;
        zone.querySelector(".drop-icon").textContent = "\u2713";
        checkReady();
    }

    function checkReady() {
        const allReady = FILE_KEYS.every(k => files[k]);
        if (allReady && !running) {
            runBtn.disabled = false;
            runBtn.classList.add("ready");
            runBtn.classList.remove("running");
        } else {
            runBtn.disabled = true;
            runBtn.classList.remove("ready");
        }
    }

    // Run button
    runBtn.addEventListener("click", async () => {
        if (running) return;
        running = true;
        runBtn.textContent = "Processing\u2026";
        runBtn.classList.remove("ready");
        runBtn.classList.add("running");
        runBtn.disabled = true;

        // Reset UI
        logPanel.innerHTML = "";
        statsPanel.style.display = "none";
        errorBox.style.display = "none";
        downloadBtn.style.display = "none";
        resetSteps();

        // Upload files
        const formData = new FormData();
        FILE_KEYS.forEach(key => {
            if (files[key]) formData.append(key, files[key]);
        });

        try {
            const uploadResp = await fetch("/upload", { method: "POST", body: formData });
            const uploadData = await uploadResp.json();
            sessionId = uploadData.session_id;

            // Start pipeline
            await fetch(`/run/${sessionId}`, { method: "POST" });

            // Connect SSE for progress
            connectSSE(sessionId);
        } catch (e) {
            showError("Upload failed: " + e.message);
            running = false;
            checkReady();
        }
    });

    function connectSSE(sid) {
        const evtSource = new EventSource(`/progress/${sid}`);

        evtSource.onmessage = (event) => {
            const data = JSON.parse(event.data);

            switch (data.type) {
                case "log":
                    appendLog(data.message);
                    break;

                case "stages":
                    updateStages(data.stages);
                    break;

                case "done":
                    showStats(data.stats);
                    downloadBtn.style.display = "block";
                    running = false;
                    runBtn.textContent = "Run Pipeline";
                    checkReady();
                    evtSource.close();
                    break;

                case "error":
                    showError(data.message);
                    running = false;
                    runBtn.textContent = "Run Pipeline";
                    checkReady();
                    evtSource.close();
                    break;
            }
        };

        evtSource.onerror = () => {
            evtSource.close();
        };
    }

    function appendLog(msg) {
        // Remove placeholder
        const placeholder = logPanel.querySelector(".log-placeholder");
        if (placeholder) placeholder.remove();

        const div = document.createElement("div");
        div.className = "log-entry";
        if (msg.includes("ERROR")) div.classList.add("error");
        else if (msg.startsWith("Stage") && msg.includes("complete")) div.classList.add("success");
        else if (msg.includes("Warning")) div.classList.add("warning");
        else if (msg.startsWith("=")) div.classList.add("highlight");

        div.textContent = msg;
        logPanel.appendChild(div);
        logPanel.scrollTop = logPanel.scrollHeight;
    }

    function updateStages(stages) {
        for (const [id, status] of Object.entries(stages)) {
            const stepEl = document.querySelector(`.step[data-id="${id}"]`);
            if (!stepEl) continue;
            stepEl.className = "step " + status;
            const badge = stepEl.querySelector(".step-badge");
            if (status === "ok") badge.textContent = "\u2713";
            else if (status === "run") badge.textContent = "\u2026";
            else if (status === "err") badge.textContent = "!";
        }
    }

    function resetSteps() {
        document.querySelectorAll(".step").forEach(el => {
            el.className = "step";
            const badge = el.querySelector(".step-badge");
            badge.textContent = el.dataset.id || badge.textContent;
        });
    }

    function showStats(stats) {
        if (!stats) return;
        document.getElementById("stat-total").textContent = (stats.total || 0).toLocaleString();
        document.getElementById("stat-mapped").textContent = (stats.mapped || 0).toLocaleString();
        document.getElementById("stat-notmapped").textContent = (stats.notMapped || 0).toLocaleString();
        document.getElementById("stat-excluded").textContent = (stats.excluded || 0).toLocaleString();
        statsPanel.style.display = "grid";
    }

    function showError(msg) {
        errorBox.textContent = msg;
        errorBox.style.display = "block";
    }

    // Download button
    downloadBtn.addEventListener("click", () => {
        if (sessionId) {
            window.location.href = `/download/${sessionId}`;
        }
    });
})();
