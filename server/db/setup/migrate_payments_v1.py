import os
import mariadb

def main():
    conn = mariadb.connect(
        host=os.getenv("DB_HOST","mariadb"),
        user=os.getenv("DB_USER","root"),
        password=os.getenv("DB_PASSWORD","teste123"),
        port=int(os.getenv("DB_PORT","3306")),
        database=os.getenv("DB_NAME","iscte_spot"),
    )
    cur = conn.cursor()

    # Column FastPayCustomerID
    try:
        cur.execute("ALTER TABLE Companies ADD COLUMN FastPayCustomerID INT(11) NULL DEFAULT NULL;")
        conn.commit()
        print("Added Companies.FastPayCustomerID")
    except mariadb.Error as e:
        print(f"ALTER Companies skipped: {e}")

    # Core payments tables
    cur.execute("""CREATE TABLE IF NOT EXISTS PaymentAccounts (
        UserID INT(11) NOT NULL,
        BankAccountNumber VARCHAR(34) NOT NULL,
        BankIdentifierCode VARCHAR(20) NOT NULL,
        CreatedAt TIMESTAMP NULL DEFAULT current_timestamp(),
        PRIMARY KEY (UserID)
    );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS CompanyCards (
        CompanyID INT(11) NOT NULL,
        FastPayCustomerID INT(11) NULL DEFAULT NULL,
        CardToken VARCHAR(128) NOT NULL,
        Last4 VARCHAR(4) NOT NULL,
        ExpiryDate VARCHAR(10) NOT NULL,
        CardType VARCHAR(20) NOT NULL,
        BankIdentifierCode VARCHAR(20) NOT NULL,
        CreatedAt TIMESTAMP NULL DEFAULT current_timestamp(),
        PRIMARY KEY (CompanyID)
    );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS PaymentSchedules (
        CompanyID INT(11) NOT NULL,
        FrequencyType VARCHAR(10) NOT NULL,
        BonusPercentage FLOAT NOT NULL DEFAULT 0,
        Enabled TINYINT(1) NOT NULL DEFAULT 1,
        UpdatedAt TIMESTAMP NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
        PRIMARY KEY (CompanyID)
    );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS PaymentAudit (
        AuditID INT(11) NOT NULL AUTO_INCREMENT,
        CompanyID INT(11) NOT NULL,
        AdminUserID INT(11) NOT NULL,
        Action VARCHAR(20) NOT NULL,
        RequestID VARCHAR(64) NOT NULL,
        Status VARCHAR(20) NOT NULL,
        Details TEXT NULL,
        CreatedAt TIMESTAMP NULL DEFAULT current_timestamp(),
        PRIMARY KEY (AuditID)
    );""")

    # ✅ API Logs conforme DDT (monitorização)
    cur.execute("""CREATE TABLE IF NOT EXISTS PaymentApiLogs (
        LogID INT(11) NOT NULL AUTO_INCREMENT,
        RequestID VARCHAR(64) NOT NULL,
        IpOrigin VARCHAR(64) NOT NULL,
        Timestamp VARCHAR(40) NOT NULL,
        Endpoint VARCHAR(128) NOT NULL,
        Method VARCHAR(10) NOT NULL,
        Headers TEXT NULL,
        Body TEXT NULL,
        UserID INT(11) NULL,
        Username VARCHAR(64) NULL,
        ResponseStatus INT(11) NOT NULL,
        ResponseBody TEXT NULL,
        PRIMARY KEY (LogID)
    );""")

    conn.commit()
    cur.close()
    conn.close()
    print("Migration completed.")

if __name__ == "__main__":
    main()
