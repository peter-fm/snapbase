package com.snapbase.demos.compliance;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot application for the Compliance Audit Dashboard
 */
@SpringBootApplication
public class ComplianceDashboardApplication {
    
    public static void main(String[] args) {
        System.out.println("Starting Compliance Audit Dashboard...");
        System.out.println("Access the dashboard at: http://localhost:8080");
        SpringApplication.run(ComplianceDashboardApplication.class, args);
    }
}