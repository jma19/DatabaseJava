package edu.uci.jun.database.rm;

import edu.uci.jun.database.DatabaseException;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class RelationManagerTest {



    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void should_create_catalog_file() throws DatabaseException {
        RelationManager relationManager =RelationManager.getInstance();
        relationManager.createCatalog();
    }



    @Test
    public void name() throws URISyntaxException {
        File file = new File("catalog");
        file.mkdir();
    }
}
