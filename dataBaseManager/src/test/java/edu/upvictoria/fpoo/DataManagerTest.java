package edu.upvictoria.fpoo;

import junit.framework.TestCase;
import org.junit.Test;

public class DataManagerTest extends TestCase {

    @Test
    public void testShowTables() {
        PathManager pManager = new PathManager();
        pManager.setDataBase("/home/xmoreno/iti-271215-poo-practica-1-xmoren025/dataBaseManager/src/main/java/edu/upvictoria/fpoo/$PATH$");

        DataManager dbManager = new DataManager(pManager);


    }
}