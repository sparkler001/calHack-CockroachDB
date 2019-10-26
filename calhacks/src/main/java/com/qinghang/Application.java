package com.qinghang;


import com.qinghang.dao.BasicDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/accounts")
public class Application {
    @Autowired
    BasicDAO dao;
    @RequestMapping("/")
    public void loginService(String[] args) {

        dao.testRetryHandling();
        dao.createAccounts();

        Map<String, String> balances = new HashMap<String, String>();
        balances.put("1", "1000");
        balances.put("2", "250");
        int updatedAccounts = dao.updateAccounts(balances);
        System.out.printf("BasicExampleDAO.updateAccounts:\n    => %s total updated accounts\n", updatedAccounts);

        // How much money is in these accounts?
        int balance1 = dao.getAccountBalance(1);
        int balance2 = dao.getAccountBalance(2);
        System.out.printf("main:\n    => Account balances at time '%s':\n    ID %s => $%s\n    ID %s => $%s\n", LocalTime.now(), 1, balance1, 2, balance2);

// Transfer $100 from account 1 to account 2
        int fromAccount = 1;
        int toAccount = 2;
        int transferAmount = 100;
        int transferredAccounts = dao.transferFunds(fromAccount, toAccount, transferAmount);
        if (transferredAccounts != -1) {
            System.out.printf("BasicExampleDAO.transferFunds:\n    => $%s transferred between accounts %s and %s, %s rows updated\n", transferAmount, fromAccount, toAccount, transferredAccounts);
        }

        balance1 = dao.getAccountBalance(1);
        balance2 = dao.getAccountBalance(2);
        System.out.printf("main:\n    => Account balances at time '%s':\n    ID %s => $%s\n    ID %s => $%s\n", LocalTime.now(), 1, balance1, 2, balance2);

        // Bulk insertion example using JDBC's batching support.
        int totalRowsInserted = dao.bulkInsertRandomAccountData();
        System.out.printf("\nBasicExampleDAO.bulkInsertRandomAccountData:\n    => finished, %s total rows inserted\n", totalRowsInserted);

        // Print out 10 account values.
        int accountsRead = dao.readAccounts(10);

        // Drop the 'accounts' table so this code can be run again.
        //dao.tearDown();
    }
}
