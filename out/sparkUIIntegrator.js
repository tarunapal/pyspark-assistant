"use strict";
/************ sparkUIIntegrator.ts ************/
Object.defineProperty(exports, "__esModule", { value: true });
exports.SparkUIIntegrator = void 0;
const node_fetch_1 = require("node-fetch");
class SparkUIIntegrator {
    constructor() {
        this.sparkUIUrl = null;
        this.connected = false;
        this.jobData = [];
        this.stageData = [];
        this.executorData = [];
        this.pollingInterval = null;
    }
    async connect(url) {
        try {
            // Test connection
            const response = await (0, node_fetch_1.default)(`${url}/api/v1/applications`);
            if (!response.ok) {
                throw new Error(`Failed to connect to Spark UI at ${url}: ${response.statusText}`);
            }
            this.sparkUIUrl = url;
            this.connected = true;
            // Start data polling
            this.startPolling();
            return true;
        }
        catch (error) {
            this.connected = false;
            throw new Error(`Failed to connect to Spark UI: ${error.message}`);
        }
    }
    async startPolling() {
        if (!this.sparkUIUrl || !this.connected) {
            return;
        }
        // Clear existing polling interval if any
        if (this.pollingInterval) {
            clearInterval(this.pollingInterval);
        }
        // Function to perform polling
        const poll = async () => {
            try {
                // Get active application ID
                const appsResponse = await (0, node_fetch_1.default)(`${this.sparkUIUrl}/api/v1/applications`);
                if (!appsResponse.ok) {
                    throw new Error('Failed to fetch applications');
                }
                const apps = await appsResponse.json();
                if (!apps || apps.length === 0) {
                    return;
                }
                const appId = apps[0].id; // Use the first/most recent application
                // Poll job data
                const jobsResponse = await (0, node_fetch_1.default)(`${this.sparkUIUrl}/api/v1/applications/${appId}/jobs`);
                if (jobsResponse.ok) {
                    this.jobData = await jobsResponse.json();
                }
                // Poll stage data
                const stagesResponse = await (0, node_fetch_1.default)(`${this.sparkUIUrl}/api/v1/applications/${appId}/stages`);
                if (stagesResponse.ok) {
                    this.stageData = await stagesResponse.json();
                }
                // Poll executor data
                const executorsResponse = await (0, node_fetch_1.default)(`${this.sparkUIUrl}/api/v1/applications/${appId}/executors`);
                if (executorsResponse.ok) {
                    this.executorData = await executorsResponse.json();
                }
            }
            catch (error) {
                console.error('Error polling Spark UI:', error.message);
            }
        };
        // Initial poll
        await poll();
        // Set up polling interval
        this.pollingInterval = setInterval(poll, 5000); // Poll every 5 seconds
    }
    disconnect() {
        this.connected = false;
        this.sparkUIUrl = null;
        if (this.pollingInterval) {
            clearInterval(this.pollingInterval);
            this.pollingInterval = null;
        }
        this.jobData = [];
        this.stageData = [];
        this.executorData = [];
    }
    getSparkUIData() {
        return {
            connected: this.connected,
            url: this.sparkUIUrl,
            jobs: this.jobData,
            stages: this.stageData,
            executors: this.executorData
        };
    }
    getJobDetails(jobId) {
        if (!this.sparkUIUrl || !this.connected) {
            throw new Error('Not connected to Spark UI');
        }
        return (0, node_fetch_1.default)(`${this.sparkUIUrl}/api/v1/applications/[app-id]/jobs/${jobId}`)
            .then((response) => {
            if (!response.ok) {
                throw new Error(`Failed to fetch job details: ${response.statusText}`);
            }
            return response.json();
        });
    }
    getStageDetails(stageId) {
        if (!this.sparkUIUrl || !this.connected) {
            throw new Error('Not connected to Spark UI');
        }
        return (0, node_fetch_1.default)(`${this.sparkUIUrl}/api/v1/applications/[app-id]/stages/${stageId}`)
            .then((response) => {
            if (!response.ok) {
                throw new Error(`Failed to fetch stage details: ${response.statusText}`);
            }
            return response.json();
        });
    }
}
exports.SparkUIIntegrator = SparkUIIntegrator;
//# sourceMappingURL=sparkUIIntegrator.js.map