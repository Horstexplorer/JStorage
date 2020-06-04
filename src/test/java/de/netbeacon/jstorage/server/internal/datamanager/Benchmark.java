/*
 *     Copyright 2020 Horstexplorer @ https://www.netbeacon.de
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.netbeacon.jstorage.server.internal.datamanager;

import de.netbeacon.jstorage.server.internal.datamanager.objects.DataBase;
import de.netbeacon.jstorage.server.internal.datamanager.objects.DataSet;
import de.netbeacon.jstorage.server.internal.datamanager.objects.DataTable;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.*;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Benchmark {

    static DataTable dataTable;

    @BeforeAll
    static void setUp() {
        try{
            File f = new File("./jstorage/data/db/");
            FileUtils.deleteDirectory(f);
            new DataManager();
            DataBase dataBase = DataManager.createDataBase("testdatabase");
            dataTable = new DataTable(dataBase, "benchmark");
            DataManager.getDataBase("testdatabase").insertTable(dataTable);
        }catch (Exception e){
            fail();}
    }

    @AfterAll
    static void tearDown() {
        try{
            DataManager.shutdown();
            File d = new File("./jstorage/data/db/");
            FileUtils.deleteDirectory(d);
        }catch (Exception e){
            fail();
        }
    }

    @Test
    @Order(1)
    void thousand_Insert(){
        assertEquals(0, dataTable.getIndexPool().size());
        for(int i = 0; i < 1000; i++){
            try{
                dataTable.insertDataSet(new DataSet(dataTable.getDataBase(), dataTable, "benchmark_"+i));
            }catch (Exception e){
                fail();
            }
        }
        assertEquals(1000, dataTable.getIndexPool().size());
    }

    @Test
    @Order(2)
    void thousand_Get(){
        assertEquals(1000, dataTable.getIndexPool().size());
        for(int i = 0; i < 1000; i++){
            try{
                DataSet dataSet = dataTable.getDataSet("benchmark_"+i);
            }catch (Exception e){
                fail();
            }
        }
        assertEquals(1000, dataTable.getIndexPool().size());
    }

    @Test
    @Order(3)
    void thousand_Delete(){
        assertEquals(1000, dataTable.getIndexPool().size());
        for(int i = 0; i < 1000; i++){
            try{
                dataTable.deleteDataSet("benchmark_"+i);
            }catch (Exception e){
                e.printStackTrace();
                fail();
            }
        }
        assertEquals(0, dataTable.getIndexPool().size());
    }

    @Test
    @Order(4)
    void tenThousand_Insert(){
        assertEquals(0, dataTable.getIndexPool().size());
        long start = System.currentTimeMillis();
        for(int i = 0; i < 10000; i++){
            if(i%1000 == 0){
                System.out.println(System.currentTimeMillis()-start);
                start = System.currentTimeMillis();
            }
            try{
                dataTable.insertDataSet(new DataSet(dataTable.getDataBase(), dataTable, "benchmark_"+i));
            }catch (Exception e){
                fail();
            }
        }
        assertEquals(10000, dataTable.getIndexPool().size());
    }

    @Test
    @Order(5)
    void tenThousand_Get(){
        assertEquals(10000, dataTable.getIndexPool().size());
        long start = System.currentTimeMillis();
        for(int i = 0; i < 10000; i++){
            if(i%1000 == 0){
                System.out.println(System.currentTimeMillis()-start);
                start = System.currentTimeMillis();
            }
            try{
                DataSet dataSet = dataTable.getDataSet("benchmark_"+i);
            }catch (Exception e){
                fail();
            }
        }
        assertEquals(10000, dataTable.getIndexPool().size());
    }

    @Test
    @Order(6)
    void tenThousand_Delete(){
        assertEquals(10000, dataTable.getIndexPool().size());
        long start = System.currentTimeMillis();
        for(int i = 0; i < 10000; i++){
            if(i%1000 == 0){
                System.out.println(System.currentTimeMillis()-start);
                start = System.currentTimeMillis();
            }
            try{
                dataTable.deleteDataSet("benchmark_"+i);
            }catch (Exception e){
                e.printStackTrace();
                fail();
            }
        }
        assertEquals(0, dataTable.getIndexPool().size());
    }

    @Test
    @Order(7)
    void fiftyThousand_Insert(){
        assertEquals(0, dataTable.getIndexPool().size());
        long start = System.currentTimeMillis();
        for(int i = 0; i < 50000; i++){
            if(i%1000 == 0){
                System.out.println(System.currentTimeMillis()-start);
                start = System.currentTimeMillis();
            }
            try{
                dataTable.insertDataSet(new DataSet(dataTable.getDataBase(), dataTable, "benchmark_"+i));
            }catch (Exception e){
                fail();
            }
        }
        assertEquals(50000, dataTable.getIndexPool().size());
    }

    @Test
    @Order(8)
    void fiftyThousand_Get(){
        assertEquals(50000, dataTable.getIndexPool().size());
        long start = System.currentTimeMillis();
        for(int i = 0; i < 50000; i++){
            if(i%1000 == 0){
                System.out.println(System.currentTimeMillis()-start);
                start = System.currentTimeMillis();
            }
            try{
                DataSet dataSet = dataTable.getDataSet("benchmark_"+i);
            }catch (Exception e){
                fail();
            }
        }
        assertEquals(50000, dataTable.getIndexPool().size());
    }

    @Test
    @Order(9)
    void fiftyThousand_Delete(){
        assertEquals(50000, dataTable.getIndexPool().size());
        long start = System.currentTimeMillis();
        for(int i = 0; i < 50000; i++){
            if(i%1000 == 0){
                System.out.println(System.currentTimeMillis()-start);
                start = System.currentTimeMillis();
            }
            try{
                dataTable.deleteDataSet("benchmark_"+i);
            }catch (Exception e){
                e.printStackTrace();
                fail();
            }
        }
        assertEquals(0, dataTable.getIndexPool().size());
    }
}
