package com.snapbase.demos.compliance;

/**
 * Customer record representing a row in the customer database.
 * Contains both regular and sensitive PII fields for compliance tracking.
 */
public class CustomerRecord {
    private Integer id;
    private String firstName;
    private String lastName;
    private String email;              // Sensitive PII
    private String ssn;                // Sensitive PII
    private String phone;              // Sensitive PII
    private String dateOfBirth;        // Sensitive PII
    private String address;            // Sensitive PII
    private Integer creditScore;       // Financial data
    private Integer annualIncome;      // Financial data
    private Double accountBalance;     // Financial data
    private String customerSince;
    
    // Constructors
    public CustomerRecord() {
    }
    
    public CustomerRecord(Integer id, String firstName, String lastName, String email, 
                         String ssn, String phone, String dateOfBirth, String address,
                         Integer creditScore, Integer annualIncome, Double accountBalance,
                         String customerSince) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.ssn = ssn;
        this.phone = phone;
        this.dateOfBirth = dateOfBirth;
        this.address = address;
        this.creditScore = creditScore;
        this.annualIncome = annualIncome;
        this.accountBalance = accountBalance;
        this.customerSince = customerSince;
    }
    
    // Getters and Setters
    public Integer getId() {
        return id;
    }
    
    public void setId(Integer id) {
        this.id = id;
    }
    
    public String getFirstName() {
        return firstName;
    }
    
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }
    
    public String getLastName() {
        return lastName;
    }
    
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }
    
    public String getEmail() {
        return email;
    }
    
    public void setEmail(String email) {
        this.email = email;
    }
    
    public String getSsn() {
        return ssn;
    }
    
    public void setSsn(String ssn) {
        this.ssn = ssn;
    }
    
    public String getPhone() {
        return phone;
    }
    
    public void setPhone(String phone) {
        this.phone = phone;
    }
    
    public String getDateOfBirth() {
        return dateOfBirth;
    }
    
    public void setDateOfBirth(String dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }
    
    public String getAddress() {
        return address;
    }
    
    public void setAddress(String address) {
        this.address = address;
    }
    
    public Integer getCreditScore() {
        return creditScore;
    }
    
    public void setCreditScore(Integer creditScore) {
        this.creditScore = creditScore;
    }
    
    public Integer getAnnualIncome() {
        return annualIncome;
    }
    
    public void setAnnualIncome(Integer annualIncome) {
        this.annualIncome = annualIncome;
    }
    
    public Double getAccountBalance() {
        return accountBalance;
    }
    
    public void setAccountBalance(Double accountBalance) {
        this.accountBalance = accountBalance;
    }
    
    public String getCustomerSince() {
        return customerSince;
    }
    
    public void setCustomerSince(String customerSince) {
        this.customerSince = customerSince;
    }
    
    @Override
    public String toString() {
        return "CustomerRecord{" +
                "id=" + id +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", email='" + maskEmail(email) + '\'' +
                ", ssn='" + maskSSN(ssn) + '\'' +
                ", phone='" + phone + '\'' +
                ", dateOfBirth='" + dateOfBirth + '\'' +
                ", address='" + address + '\'' +
                ", creditScore=" + creditScore +
                ", annualIncome=" + annualIncome +
                ", accountBalance=" + accountBalance +
                ", customerSince='" + customerSince + '\'' +
                '}';
    }
    
    private String maskEmail(String email) {
        if (email == null || !email.contains("@")) {
            return "***@***.com";
        }
        int atIndex = email.indexOf('@');
        return email.substring(0, Math.min(2, atIndex)) + "***" + email.substring(atIndex);
    }
    
    private String maskSSN(String ssn) {
        if (ssn == null || ssn.length() < 4) {
            return "***-**-****";
        }
        return "***-**-" + ssn.substring(ssn.length() - 4);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        CustomerRecord that = (CustomerRecord) obj;
        return java.util.Objects.equals(id, that.id);
    }
    
    @Override
    public int hashCode() {
        return java.util.Objects.hash(id);
    }
}