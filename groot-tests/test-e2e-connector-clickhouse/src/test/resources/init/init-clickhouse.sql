show databases;
-- ALTER USER default IDENTIFIED WITH plaintext_password BY 'testuser';
CREATE USER testuser IDENTIFIED WITH plaintext_password BY 'testuser';
GRANT ALL ON *.* TO testuser;