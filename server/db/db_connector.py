import os
import mariadb
class DBConnector:

    def __init__(self):
        self.host = os.getenv('DB_HOST', 'mariadb')
        self.user = os.getenv('DB_USER', 'root')
        self.password = os.getenv('DB_PASSWORD', 'teste123')
        self.database = os.getenv('DB_NAME', 'iscte_spot')
        self.port = int(os.getenv('DB_PORT', '3306'))

    def connect(self):
        ''' Connect to database mariadb'''
        try:
            connection = mariadb.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
            )
            return connection
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            return None

    def execute_query(self, query, args=None):
        ''' Execute queries by query name
            (mantive a tua docstring original para não mexer no resto do projeto)
        '''
        print(f'DB query selceted: {query}, args: {args}')
        connection = self.connect()
        if connection is None:
            return None

        cursor = connection.cursor(dictionary=True)
        result = None
        try:
            if query == 'get_user_by_name':
                cursor.execute("SELECT UserID FROM Users WHERE Username = ?", (args,))
                result = cursor.fetchone()
                print("Result: ")
                print(result)
                try:
                    if isinstance(result, tuple):
                        result = result[0]['UserID']
                        if result == 1:
                            return True
                    else:
                        result = result["UserID"]
                        if result == 0:
                            return False
                except TypeError:
                    return 'TypeError'

            elif query == 'get_user_password':
                cursor.execute("SELECT PasswordHash FROM Users WHERE UserID = ?", (args,))
                result = cursor.fetchone()
                try:
                    if isinstance(result, tuple):
                        return result[0]['PasswordHash']
                    else:
                        return result['PasswordHash']
                except TypeError:
                    return False

            elif query == 'get_user_by_id':
                cursor.execute("SELECT * FROM Users WHERE UserID = ?", (args,))
                result = cursor.fetchone()

            elif query == 'get_clients_list':
                cursor.execute(
                    """
                    SELECT ClientID, FirstName, LastName, Email, PhoneNumber, Address, City, Country
                    FROM Clients
                    WHERE CompanyID = ?
                    """,
                    (args,)
                )
                result = cursor.fetchall()
                if isinstance(result, list):
                    return result
                else:
                    return False

            elif query == 'get_employees_list':
                cursor.execute("SELECT UserID, Username, Email, CommissionPercentage, isActive FROM Users WHERE CompanyID = ?", (args,))
                result = cursor.fetchall()
                if isinstance(result, list):
                    return result
                else:
                    return False

            elif query == 'get_compnay_id_by_user':
                cursor.execute("SELECT CompanyID FROM Users WHERE UserID = ?", (args,))
                result = cursor.fetchone()
                print(result)
                if isinstance(result, tuple):
                    return result[0]['CompanyID']
                else:
                    return result["CompanyID"]

            elif query == 'get_company_sales':
                cursor.execute(
                    """
                    SELECT Sales.SaleID, Products.ProductName, Users.Username, Clients.FirstName, Clients.FirstName, Products.SellingPrice, Sales.Quantity, Sales.SaleDate
                    FROM Sales
                    JOIN Clients ON Sales.ClientID = Clients.ClientID
                    JOIN Users ON Sales.UserID = Users.UserID
                    JOIN Products ON Sales.ProductID = Products.ProductID
                    WHERE Clients.CompanyID = ?;
                    """,
                    (args,)
                )
                result = cursor.fetchall()
                if isinstance(result, list):
                    return result
                else:
                    return False

            elif query == 'get_user_sales':
                cursor.execute(
                    """
                    SELECT 
                        S.SaleID,
                        U.UserName,    
                        C.FirstName,   
                        P.ProductName,
                        P.SellingPrice,
                        S.Quantity,
                        S.SaleDate
                    FROM 
                        Sales S
                    JOIN 
                        Users U ON S.UserID = U.UserID
                    JOIN 
                        Clients C ON S.ClientID = C.ClientID
                    JOIN 
                        Products P ON S.ProductID = P.ProductID
                    WHERE 
                        S.UserID = ?;
                    """,
                    (args,)
                )
                result = cursor.fetchall()
                print(result)
                if isinstance(result, list):
                    return result
                else:
                    return False

            elif query == 'get_user_admin':
                cursor.execute(
                    "SELECT IsAdmin FROM Users WHERE UserID = ?;",
                    (args,)
                )
                result = cursor.fetchone()
                print(result)
                if isinstance(result, tuple):
                    return result[0]['IsAdmin']
                else:
                    return result['IsAdmin']

            elif query == 'get_user_comp_id':
                cursor.execute(
                    "SELECT CompanyID FROM Users WHERE UserID = ?;",
                    (args,)
                )
                result = cursor.fetchone()
                print(result)
                if isinstance(result, tuple):
                    return result[0]['CompanyID']
                else:
                    return result['CompanyID']

            elif query == 'get_products_list':
                cursor.execute("SELECT ProductID, ProductName, SellingPrice FROM Products WHERE CompanyID = ?", (args,))
                result = cursor.fetchall()
                if isinstance(result, list):
                    return result
                else:
                    return False

            elif query == 'get_company_revenue':
                cursor.execute("SELECT Revenue FROM Companies WHERE CompanyID = ?", (args,))
                result = cursor.fetchone()
                if isinstance(result, tuple):
                    return result[0]
                return result['Revenue']

            elif query == 'get_employees_return':
                cursor.execute(
                    """
                    SELECT 
                        u.UserID,
                        u.Username,
                        u.CommissionPercentage,
                        COUNT(s.SaleID) AS total_sales,
                        SUM(s.Quantity * p.SellingPrice) AS total_sales_amount,
                        (SUM(s.Quantity * p.SellingPrice) * (u.CommissionPercentage / 100)) AS total_commission
                    FROM Users u
                    LEFT JOIN Sales s ON u.UserID = s.UserID
                    LEFT JOIN Products p ON s.ProductID = p.ProductID
                    WHERE u.CompanyID = ? 
                      AND p.CompanyID = u.CompanyID
                      AND MONTH(s.SaleDate) = ?
                      AND YEAR(s.SaleDate) = 2024
                    GROUP BY u.UserID, u.CommissionPercentage
                    """,
                    (args['comp_id'], args['month'])
                )
                result = cursor.fetchall()
                employee_sales_data = []
                print(f'Result: {result}')
                for row in result:
                    employee_sales_data.append({
                        "UserID": row['UserID'],
                        "Username": row['Username'],
                        "CommissionPercentage": row['CommissionPercentage'],
                        "TotalSales": row['total_sales'],
                        "TotalSalesAmount": row['total_sales_amount'],
                        "TotalCommission": row['total_commission']
                    })
                return employee_sales_data

            elif query == 'get_last_3_sales':
                cursor.execute(
                    """
                    SELECT 
                        S.SaleID,
                        U.UserName,    
                        C.FirstName,   
                        P.ProductName,
                        P.SellingPrice,
                        S.Quantity,
                        S.SaleDate
                    FROM 
                        Sales S
                    JOIN
                        Users U ON S.UserID = U.UserID
                    JOIN 
                        Clients C ON S.ClientID = C.ClientID
                    JOIN 
                        Products P ON S.ProductID = P.ProductID
                    WHERE 
                        S.UserID = ?
                    ORDER BY
                        S.SaleDate DESC
                    LIMIT 3;
                    """,
                    (args,)
                )
                result = cursor.fetchall()
                print(result)
                if isinstance(result, list):
                    return result
                else:
                    return False

            elif query == 'get_sales_month_comp_id':
                cursor.execute(
                    """
                    SELECT 
                        Sales.SaleID,
                        Sales.UserID,
                        Sales.ClientID,
                        Sales.ProductID,
                        Sales.Quantity,
                        Sales.SaleDate
                    FROM 
                        Sales
                    JOIN 
                        Users ON Sales.UserID = Users.UserID
                    WHERE 
                        Users.CompanyID = ?
                        AND MONTH(Sales.SaleDate) = ?
                        AND YEAR(Sales.SaleDate) = 2024;
                    """,
                    (args['comp_id'], args['month'])
                )
                result = cursor.fetchall()
                if isinstance(result, list):
                    return result
                else:
                    return False

            elif query == 'get_costs_sales_month':
                cursor.execute(
                    """
                    SELECT 
                        SUM(Products.SellingPrice * Sales.Quantity) AS TotalSellingPrice,
                        SUM(Products.FactoryPrice * Sales.Quantity) AS TotalFactoryPrice
                    FROM 
                        Sales
                    JOIN 
                        Products ON Sales.ProductID = Products.ProductID
                    WHERE 
                        Products.CompanyID = ?
                        AND MONTH(Sales.SaleDate) = ?
                        AND YEAR(Sales.SaleDate) = 2024;
                    """,
                    (args['comp_id'], args['month'])
                )
                result = cursor.fetchone()
                print(result)
                try:
                    if isinstance(result, tuple):
                        return result[0]
                    else:
                        return result
                except TypeError:
                    return False

            elif query == 'get_admin_tickets':
                cursor.execute(
                    """
                    SELECT 
                        st.TicketID,
                        st.UserID,
                        u.CompanyID,
                        st.Status,
                        st.Category,
                        st.Description,
                        st.Messages,
                        st.CreatedAt,
                        st.UpdatedAt
                    FROM 
                        SupportTickets st
                    JOIN 
                        Users u ON st.UserID = u.UserID
                    WHERE 
                        u.CompanyID = ?;
                    """,
                    (args,)
                )
                result = cursor.fetchall()
                if isinstance(result, list):
                    return result
                else:
                    return False

            elif query == 'get_user_tickets':
                cursor.execute(
                    """
                    SELECT 
                        TicketID,
                        UserID,
                        Status,
                        Category,
                        Description,
                        Messages,
                        CreatedAt,
                        UpdatedAt
                    FROM 
                        SupportTickets st
                    WHERE 
                        UserID = ?;
                    """,
                    (args,)
                )
                result = cursor.fetchall()
                if isinstance(result, list):
                    return result
                else:
                    return False

            elif query == 'get_user_agent':
                cursor.execute(
                    "SELECT IsAgent FROM Users WHERE UserID = ?;",
                    (args,)
                )
                result = cursor.fetchone()
                print(result)
                try:
                    return result['IsAgent'] == 1
                except TypeError:
                    return False

            elif query == 'get_ticket_by_id':
                cursor.execute(
                    "SELECT * FROM SupportTickets WHERE TicketID = ?;",
                    (args,)
                )
                result = cursor.fetchone()
                print(result)
                try:
                    if isinstance(result, tuple):
                        return result[0]
                    else:
                        return result
                except TypeError:
                    return False

            elif query == 'get_agent_tickets':
                cursor.execute('SELECT * From SupportTickets')
                result = cursor.fetchall()
                print(result)
                if isinstance(result, list):
                    return result
                else:
                    return False

            elif query == 'create_user_employee':
                # Idempotente: se username/email já existir, devolve o UserID existente
                try:
                    cursor.execute(
                        """
                        INSERT INTO Users (Username, PasswordHash, Email, CompanyID, CommissionPercentage, ResetPassword, CreatedAt)
                        VALUES (?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
                        """,
                        (args['username'], 'T3MP-password-32', args['email'], args['comp_id'], 5)
                    )
                    connection.commit()
                    return cursor.lastrowid

                except mariadb.Error as e:
                    # 1062 = Duplicate entry (username/email UNIQUE)
                    if getattr(e, "errno", None) == 1062:
                        cursor.execute(
                            "SELECT UserID, CompanyID FROM Users WHERE Username = ? OR Email = ? LIMIT 1",
                            (args['username'], args['email'])
                        )
                        row = cursor.fetchone()
                        if row:
                            user_id = row["UserID"]

                            # garante que fica ligado à empresa correta + marca reset
                            cursor.execute(
                                "UPDATE Users SET CompanyID = ?, ResetPassword = 1 WHERE UserID = ?",
                                (args['comp_id'], user_id)
                            )
                            connection.commit()
                            return user_id
                    raise


            elif query == 'create_user_admin':
                # Idempotente: se username/email já existir, devolve o UserID existente
                try:
                    is_admin = 1 if args.get('is_admin', True) else 0
                    cursor.execute(
                        "INSERT INTO Users (Username, PasswordHash, Email, IsAdmin, CreatedAt) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
                        (args['username'], args['password'], args['email'], is_admin)
                    )
                    connection.commit()
                    return cursor.lastrowid

                except mariadb.Error as e:
                    # 1062 = Duplicate entry (username/email UNIQUE)
                    if getattr(e, "errno", None) == 1062:
                        cursor.execute(
                            "SELECT UserID FROM Users WHERE Username = ? OR Email = ? LIMIT 1",
                            (args['username'], args['email'])
                        )
                        row = cursor.fetchone()
                        if row:
                            user_id = row["UserID"]
                            # garante que fica admin (porque é signup admin)
                            cursor.execute("UPDATE Users SET IsAdmin = 1 WHERE UserID = ?", (user_id,))
                            connection.commit()
                            return user_id
                    raise

            elif query == 'create_company':
                # Idempotente:
                # 1) se o user já tiver CompanyID, reusa
                cursor.execute("SELECT CompanyID FROM Users WHERE UserID = ? LIMIT 1", (args['user_id'],))
                row = cursor.fetchone()
                if row and row.get("CompanyID"):
                    return row["CompanyID"]

                # 2) se já existir company criada para este admin, reusa (evita duplicar)
                cursor.execute(
                    "SELECT CompanyID FROM Companies WHERE AdminUserID = ? ORDER BY CompanyID DESC LIMIT 1",
                    (args['user_id'],)
                )
                row2 = cursor.fetchone()
                if row2 and row2.get("CompanyID"):
                    return row2["CompanyID"]

                # 3) cria nova
                cursor.execute(
                    "INSERT INTO Companies (CompanyName, NumberOfEmployees, AdminUserID, Revenue) VALUES (?, ?, ?, ?)",
                    (args['comp_name'], args['num_employees'], args['user_id'], 0)
                )
                connection.commit()
                return cursor.lastrowid



            elif query == 'create_client':
                cursor.execute(
                    "INSERT INTO Clients (FirstName, LastName, Email, PhoneNumber, Address, City, Country, CompanyID, CreatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
                    (args['first_name'], args['last_name'], args['email'], args['phone_number'], args['address'], args['city'], args['country'], args['comp_id'])
                )
                connection.commit()
                result = cursor.lastrowid
                if isinstance(result, tuple):
                    return result[0]
                else:
                    return result

            elif query == 'create_sale':
                cursor.execute(
                    "INSERT INTO Sales (UserID, ClientID, ProductID, Quantity, SaleDate) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
                    (args['user_id'], args['client_id'], args['product_id'], args['quantity'])
                )
                connection.commit()
                return True

            elif query == 'create_ticket':
                cursor.execute(
                    "INSERT INTO SupportTickets (UserID, Status, Category, Description, Messages, CreatedAt) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
                    (args['user_id'], args['status'], args['category'], args['description'], args['messages'])
                )
                connection.commit()
                result = cursor.lastrowid
                if isinstance(result, tuple):
                    return result[0]
                else:
                    return result

            elif query == 'update_user_password':
                cursor.execute(
                    "UPDATE Users SET PasswordHash = ? WHERE UserID = ?;",
                    (args["new_password"], args["user_id"])
                )
                connection.commit()
                if cursor.rowcount > 0:
                    return True
                else:
                    return False

            elif query == 'update_user_comp_id':
                # Idempotente: se já tiver o mesmo CompanyID, considera OK
                cursor.execute("SELECT CompanyID FROM Users WHERE UserID = ? LIMIT 1", (args["user_id"],))
                row = cursor.fetchone()
                current = row.get("CompanyID") if row else None

                if current == args["comp_id"]:
                    return True

                cursor.execute(
                    "UPDATE Users SET CompanyID = ? WHERE UserID = ?;",
                    (args["comp_id"], args["user_id"])
                )
                connection.commit()
                return True



            elif query == 'update_user_activity':
                if args['active']:
                    cursor.execute("UPDATE Users SET LastLogin = CURRENT_TIMESTAMP, isActive = True WHERE UserID = ?", (args['user_id'],))
                else:
                    cursor.execute("UPDATE Users SET LastLogout = CURRENT_TIMESTAMP, isActive = False WHERE UserID = ?", (args['user_id'],))
                connection.commit()
                return cursor.rowcount

            elif query == 'update_products_by_comp_id':
                cursor.execute(
                    "DELETE FROM Products WHERE CompanyID = ?",
                    (args['comp_id'],)
                )

                insert_query = """
                    INSERT INTO Products (ProductID, CompanyID, ProductName, FactoryPrice, SellingPrice, CreatedAt)
                    VALUES (?, ?, ?, ?, ?, ?)
                """

                for _index, row in args['file'].iterrows():
                    cursor.execute(insert_query, (row['ProductID'], args['comp_id'], row['ProductName'], row['FactoryPrice'], row['SellingPrice'], row['CreatedAt']))

                connection.commit()
                return True

            elif query == 'update_company_revenue':
                cursor.execute(
                    """
                    SELECT SUM(s.Quantity * p.SellingPrice) AS total_sales
                    FROM Sales s
                    JOIN Products p ON s.ProductID = p.ProductID
                    JOIN Users u ON s.UserID = u.UserID
                    WHERE u.CompanyID = ?;
                    """,
                    (args,)
                )
                result = cursor.fetchone()
                if isinstance(result, dict):
                    result = result['total_sales']

                cursor.execute(
                    """
                    UPDATE Companies
                    SET Revenue = ?
                    WHERE CompanyID = ?
                    """,
                    (result, args)
                )
                connection.commit()
                return cursor.rowcount > 0

            elif query == 'update_ticket_messages':
                message = args["message"]
                username = args['username']
                ticket_id = args['ticket_id']
                new_status = 'Waiting for customer' if args['is_agent'] else 'Waiting for support'

                cursor.execute(
                    """
                    UPDATE SupportTickets
                    SET 
                        Messages = JSON_ARRAY_APPEND(
                            IFNULL(Messages, JSON_ARRAY()), '$', JSON_OBJECT('Username', %s, 'MessageText', %s)
                        ),
                        UpdatedAt = CURRENT_TIMESTAMP,
                        Status = %s
                    WHERE TicketID = %s;
                    """, (username, message, new_status, ticket_id)
                )

                connection.commit()
                return cursor.rowcount >= 0

            elif query == 'update_ticket_status':
                cursor.execute(
                    """
                    UPDATE SupportTickets
                    SET Status = %s, UpdatedAt = CURRENT_TIMESTAMP
                    WHERE TicketID = %s
                    """,
                    (args['status'], args['ticket_id'])
                )
                connection.commit()
                return cursor.rowcount >= 0

            elif query == 'delete_sales_by_comp_id':
                cursor.execute(
                    """
                    DELETE FROM Sales
                    WHERE UserID IN (
                        SELECT UserID FROM Users WHERE CompanyID = ?
                    );
                    """,
                    (args,)
                )
                connection.commit()
                return True
            
            elif query == 'insert_payment_api_log':
                cursor.execute(
                    """INSERT INTO PaymentApiLogs
                       (RequestID, IpOrigin, Timestamp, Endpoint, Method, Headers, Body, UserID, Username, ResponseStatus, ResponseBody)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        args["request_id"],
                        args["ip"],
                        args["timestamp"],
                        args["endpoint"],
                        args["method"],
                        args.get("headers"),
                        args.get("body"),
                        args.get("user_id"),
                        args.get("username"),
                        args["response_status"],
                        args.get("response_body"),
                    )
                )
                connection.commit()
                return cursor.lastrowid


            elif query == 'update_seller_commission':
                value = args['new_commission']
                user_id = args['seller_id']
                cursor.execute(
                    """
                    UPDATE Users
                    SET CommissionPercentage = ?
                    WHERE UserID = ?;
                    """,
                    (value, user_id)
                )
                connection.commit()
                return cursor.rowcount

            elif query == 'delete_products_by_comp_id':
                cursor.execute("DELETE FROM Products WHERE CompanyID = ?", (args,))
                connection.commit()
                return True

            elif query == 'delete_users_by_comp_id':
                cursor.execute("DELETE FROM Users WHERE CompanyID = ?", (args,))
                connection.commit()
                return True

            elif query == 'delete_user_by_id':
                cursor.execute("DELETE FROM Users WHERE UserID = ?", (args,))
                connection.commit()
                return cursor.rowcount > 0

            elif query == 'delete_company_by_id':
                cursor.execute("DELETE FROM Companies WHERE CompanyID = ?", (args,))
                connection.commit()
                return cursor.rowcount > 0

            elif query == 'delete_client_by_id':
                # FIX: SQL Injection
                cursor.execute("DELETE FROM Clients WHERE ClientID = ?", (args,))
                connection.commit()
                return cursor.rowcount > 0

            # =========================
            # NEW QUERIES: Payments
            # =========================
            elif query == 'set_fastpay_customer_id':
                cursor.execute(
                    "UPDATE Companies SET FastPayCustomerID = ? WHERE CompanyID = ?",
                    (args["customer_id"], args["comp_id"])
                )
                connection.commit()
                return cursor.rowcount >= 0

            elif query == 'get_fastpay_customer_id':
                cursor.execute("SELECT FastPayCustomerID FROM Companies WHERE CompanyID = ?", (args,))
                row = cursor.fetchone()
                if not row:
                    return None
                return row.get("FastPayCustomerID")

            elif query == 'upsert_company_card':
                cursor.execute(
                    """INSERT INTO CompanyCards(CompanyID, FastPayCustomerID, CardToken, Last4, ExpiryDate, CardType, BankIdentifierCode)
                       VALUES(?, ?, ?, ?, ?, ?, ?)
                       ON DUPLICATE KEY UPDATE
                         FastPayCustomerID=VALUES(FastPayCustomerID),
                         CardToken=VALUES(CardToken),
                         Last4=VALUES(Last4),
                         ExpiryDate=VALUES(ExpiryDate),
                         CardType=VALUES(CardType),
                         BankIdentifierCode=VALUES(BankIdentifierCode)
                    """,
                    (args["comp_id"], args["customer_id"], args["card_token"], args["last4"], args["expiry_date"], args["card_type"], args["bic"])
                )
                connection.commit()
                return cursor.rowcount >= 0

            elif query == 'upsert_payment_account':
                cursor.execute(
                    """INSERT INTO PaymentAccounts(UserID, BankAccountNumber, BankIdentifierCode)
                       VALUES(?, ?, ?)
                       ON DUPLICATE KEY UPDATE
                         BankAccountNumber=VALUES(BankAccountNumber),
                         BankIdentifierCode=VALUES(BankIdentifierCode)
                    """,
                    (args["user_id"], args["bank_account_number"], args["bic"])
                )
                connection.commit()
                return cursor.rowcount >= 0

            elif query == 'upsert_payment_schedule':
                cursor.execute(
                    """INSERT INTO PaymentSchedules(CompanyID, FrequencyType, BonusPercentage, Enabled)
                       VALUES(?, ?, ?, 1)
                       ON DUPLICATE KEY UPDATE
                         FrequencyType=VALUES(FrequencyType),
                         BonusPercentage=VALUES(BonusPercentage),
                         Enabled=1
                    """,
                    (args["comp_id"], args["frequency_type"], args["bonus_percentage"])
                )
                connection.commit()
                return cursor.rowcount >= 0

            elif query == 'insert_payment_audit':
                cursor.execute(
                    """INSERT INTO PaymentAudit(CompanyID, AdminUserID, Action, RequestID, Status, Details)
                       VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    (args["company_id"], args["admin_user_id"], args["action"], args["request_id"], args["status"], args.get("details", ""))
                )
                connection.commit()
                return cursor.lastrowid

            elif query == 'get_payment_targets':
                cursor.execute(
                    """SELECT PA.BankAccountNumber as bank_account_number,
                              ROUND((SUM(S.Quantity * P.SellingPrice) * (U.CommissionPercentage/100.0)) * (1 + (?/100.0)), 2) AS amount
                       FROM Users U
                       JOIN Sales S ON S.UserID = U.UserID
                       JOIN Products P ON P.ProductID = S.ProductID
                       JOIN PaymentAccounts PA ON PA.UserID = U.UserID
                       WHERE U.CompanyID = ? AND U.IsAdmin = 0 AND U.IsAgent = 0
                       GROUP BY PA.BankAccountNumber, U.UserID
                    """,
                    (args["bonus_pct"], args["comp_id"])
                )
                rows = cursor.fetchall() or []
                return [(r["bank_account_number"], float(r["amount"])) for r in rows]

        except mariadb.Error as e:
            print(f"Error: {e}")
            result = None
        finally:
            cursor.close()
            connection.close()
        return result
