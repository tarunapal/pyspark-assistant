/************ sparkUIIntegrator.ts ************/

import * as vscode from 'vscode';
import fetch, { Response } from 'node-fetch';

export class SparkUIIntegrator {
    private sparkUIUrl: string | null = null;
    private connected: boolean = false;
    private jobData: any[] = [];
    private stageData: any[] = [];
    private executorData: any[] = [];
    private pollingInterval: ReturnType<typeof setInterval> | null = null;
    
    async connect(url: string): Promise<boolean> {
        try {
            // Test connection
            const response = await fetch(`${url}/api/v1/applications`);
            if (!response.ok) {
                throw new Error(`Failed to connect to Spark UI at ${url}: ${response.statusText}`);
            }
            
            this.sparkUIUrl = url;
            this.connected = true;
            
            // Start data polling
            this.startPolling();
            
            return true;
        } catch (error: any) {
            this.connected = false;
            throw new Error(`Failed to connect to Spark UI: ${error.message}`);
        }
    }

    private async startPolling(): Promise<void> {
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
                const appsResponse = await fetch(`${this.sparkUIUrl}/api/v1/applications`);
                if (!appsResponse.ok) {
                    throw new Error('Failed to fetch applications');
                }
                
                const apps = await appsResponse.json();
                if (!apps || apps.length === 0) {
                    return;
                }
                
                const appId = apps[0].id; // Use the first/most recent application

                // Poll job data
                const jobsResponse = await fetch(`${this.sparkUIUrl}/api/v1/applications/${appId}/jobs`);
                if (jobsResponse.ok) {
                    this.jobData = await jobsResponse.json();
                }

                // Poll stage data
                const stagesResponse = await fetch(`${this.sparkUIUrl}/api/v1/applications/${appId}/stages`);
                if (stagesResponse.ok) {
                    this.stageData = await stagesResponse.json();
                }

                // Poll executor data
                const executorsResponse = await fetch(`${this.sparkUIUrl}/api/v1/applications/${appId}/executors`);
                if (executorsResponse.ok) {
                    this.executorData = await executorsResponse.json();
                }
            } catch (error: any) {
                console.error('Error polling Spark UI:', error.message);
            }
        };

        // Initial poll
        await poll();

        // Set up polling interval
        this.pollingInterval = setInterval(poll, 5000); // Poll every 5 seconds
    }

    public disconnect(): void {
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

    public getSparkUIData() {
        return {
            connected: this.connected,
            url: this.sparkUIUrl,
            jobs: this.jobData,
            stages: this.stageData,
            executors: this.executorData
        };
    }

    public getJobDetails(jobId: string) {
        if (!this.sparkUIUrl || !this.connected) {
            throw new Error('Not connected to Spark UI');
        }

        return fetch(`${this.sparkUIUrl}/api/v1/applications/[app-id]/jobs/${jobId}`)
            .then((response: Response) => {
                if (!response.ok) {
                    throw new Error(`Failed to fetch job details: ${response.statusText}`);
                }
                return response.json();
            });
    }

    public getStageDetails(stageId: string) {
        if (!this.sparkUIUrl || !this.connected) {
            throw new Error('Not connected to Spark UI');
        }

        return fetch(`${this.sparkUIUrl}/api/v1/applications/[app-id]/stages/${stageId}`)
            .then((response: Response) => {
                if (!response.ok) {
                    throw new Error(`Failed to fetch stage details: ${response.statusText}`);
                }
                return response.json();
            });
    }
}