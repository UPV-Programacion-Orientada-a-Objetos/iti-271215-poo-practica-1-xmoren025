package edu.upvictoria.fpoo;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DataManager {
    public File directory;
    public String path;

    public DataManager(){
    }

    public void procesarEntrada(String entrada) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            while (!(entrada = br.readLine().trim()).equalsIgnoreCase("exit")) {
                if (entrada.toUpperCase().startsWith("USE")) {
                    use(entrada);
                } else if (path.isEmpty()) {
                    System.out.println("Debe determinar una ruta de trabajo antes de hacer uso de la base de datos.");
                } else if (entrada.toUpperCase().startsWith("SHOW TABLES")) {
                    showTables();
                } else if (entrada.toUpperCase().startsWith("CREATE TABLE")) {
                    createTable();
                } else if (entrada.toUpperCase().startsWith("DROP TABLE")) {
                    drop();
                } else if (entrada.toUpperCase().startsWith("INSERT INTO")) {
                    insert();
                } else if (entrada.toUpperCase().startsWith("DELETE FROM")) {
                    delete();
                } else if (entrada.toUpperCase().startsWith("SELECT")) {
                    select();
                } else if (entrada.toUpperCase().startsWith("UPDATE")) {
                    update();
                } else {
                    System.out.println("No se reconoce el comando.");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLSintaxisException e) {
            throw new RuntimeException(e);
        } catch (NoTablesException e) {
            throw new RuntimeException(e);
        }
    }

    //USE
    public void use(String entrada) {
        String path = entrada.substring(4).trim();
        Path myPath = Paths.get(path).toAbsolutePath();

        if(!Files.exists(myPath) || !Files.isDirectory(myPath)){
            throw new IllegalArgumentException("La ruta no es válida.");
        }

        String nombreDirectory = myPath.getFileName().toString();
        Path newDirectory = Paths.get("./"+ nombreDirectory);

        System.out.println("La ruta de trabajo se estableció correctamente. :)");

        try{
            Files.createDirectory(newDirectory);
            System.out.println("Se ha creado la Base de Datos correctamente.");
        }catch(IOException e) {
            throw new IllegalArgumentException(e);
        }

    }

    // CREATE TABLE
    public void createTable(String entrada) throws SQLSintaxisException {
        // Dividir la consulta
        String[] entradaSplit = entrada.trim().split("\\s+");

        if (!entradaSplit[0].equalsIgnoreCase("CREATE") || !entradaSplit[1].equalsIgnoreCase("TABLE")) {
            throw new SQLSintaxisException();
        }

        // Obtener la lista de columnas
        String tableName = entradaSplit[2].trim();
        List<String> columns = new ArrayList<>();

       int startColumns = entrada.indexOf("(");
       if (startColumns ==-1){
           throw new SQLSintaxisException();
       }

       String columnsPart = entrada.substring(startColumns+1,entrada.lastIndexOf(")")).trim();
       String[] columnas = columnsPart.split(",");
       for (String column : columnas){
           String columnTrim = column.trim();
           if (!columnTrim.isEmpty()) {
               columns.add(columnTrim);
           }
       }

       File tablaFile = new File(path+ "/" + tableName + ".csv");
       if (tablaFile.exists()){
           System.out.println("La tabla ya existe");
           return;
       }

       try(BufferedWriter writer = new BufferedWriter(new FileWriter(tablaFile))){
           writer.write(String.join(",",columns));
           System.out.println("La tabla se creó con exito.");
       } catch (IOException e){}
    }


    //SHOW TABLES
    public List<String> showTables() throws NoTablesException{
        List<String> tableNames = new ArrayList<>();
        System.out.println("Tablas en la Base de Datos:");
        File[] tablas = directory.listFiles();
        if (tablas !=null){
            for (File tabla :tablas){
                if(tabla.isFile() && tabla.getName().endsWith(".csv")){
                    String tableName = tabla.getName().substring(0,tabla.getName().length()-4);
                    tableNames.add(tableName);
                    System.out.println(tableName);
                }
            }
            if (tableNames.isEmpty()){
                throw new NoTablesException("No hay tablas en la Base de datos");
            }
        } else {
            throw new NoTablesException("No hay tablas en la Base de datos");
        }
        return tableNames;
    }

    // INSERT
    public void insert(String entrada){

    }

    // DROP
    public void drop(String entrada){

    }

    // UPDATE
    public void update(String sqlConsulta){

    }
    // SELECT
    public List<List<String>> select(String sqlConsulta) throws IllegalArgumentException, IOException{
        return null;
    }
    //DELETE
    public void delete(String sqlConsulta) throws IllegalArgumentException, IOException{

    }
}
