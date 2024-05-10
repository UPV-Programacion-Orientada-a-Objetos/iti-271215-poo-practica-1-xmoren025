package edu.upvictoria.fpoo;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class DataManager {
    public File directory;
    public String path;

    public DataManager(){
    path = "";
    }

    public void procesarEntrada(String entrada) throws SQLSintaxisException, NoTablesException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            while (!(entrada = br.readLine().trim()).equalsIgnoreCase("exit")) {
                if (entrada.toUpperCase().startsWith("USE")) {
                    String[] parts = entrada.split("\\s+");
                    if (parts.length != 2) {
                        throw new SQLSintaxisException();
                    }
                    String path = parts[1].trim();
                    use(path); // Llama al método use() para establecer la ruta de trabajo
                } else if (path.isEmpty()) {
                    System.out.println("Debe determinar una ruta de trabajo antes de hacer uso de la base de datos.");
                } else if (entrada.toUpperCase().startsWith("SHOW TABLES")) {
                    showTables();
                } else if (entrada.toUpperCase().startsWith("CREATE TABLE")) {
                    createTable(entrada);
                } else if (entrada.toUpperCase().startsWith("DROP TABLE")) {
                    drop(entrada);
                } else if (entrada.toUpperCase().startsWith("INSERT INTO")) {
                    insert(entrada);
                } else if (entrada.toUpperCase().startsWith("DELETE FROM")) {
                    delete(entrada);
                } else if (entrada.toUpperCase().startsWith("SELECT")) {
                    select(entrada);
                } else if (entrada.toUpperCase().startsWith("UPDATE")) {
                    update(entrada);
                } else {
                    System.out.println("No se reconoce el comando.");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //USE
    public void use(String query) throws SQLSintaxisException {
        query = query.trim();
        if (!path.isEmpty()) {
            System.err.println("La base de datos ya fue seleccionada previamente");
            return;
        }
        String carpetaStr = query.substring(3).trim();
        if (carpetaStr.isEmpty()) {
            throw new SQLSintaxisException();
        }
        path = carpetaStr;
        if (path.endsWith(" ")) {
            path = path.substring(0, path.length() - 1).trim();
        }
        directory = new File(path);
        if (directory.isDirectory()) {
            System.out.println("Base de datos seleccionada exitosamente.");
        } else {
            System.err.println("La base de datos no existe o no es un directorio.");
            path = "";
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
    public void showTables() throws NoTablesException{
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
                throw new NoTablesException();
            }
        } else {
            throw new NoTablesException();
        }
    }

    // INSERT
    public void insert(String entrada) throws SQLSintaxisException {
        // Dividir la consulta
        String[] entradaSplit = entrada.trim().split("\\s+");
        if (entradaSplit.length <5 || !entradaSplit[3].equals("(")|| !entradaSplit[entradaSplit.length-2].equals(")")){
            throw new SQLSintaxisException();
        }

        String tableName = entradaSplit[2];
        File tableFile = new File(path+"/"+ tableName+".csv");
        if (!tableFile.exists()){
            System.out.println("La tabla no existe");
            return;
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile, true));
             BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {

            String headersLine = reader.readLine();
            String[] headers = headersLine.split(",");

            String valuesPart = entrada.substring(entrada.indexOf('(') + 1, entrada.lastIndexOf(')'));
            String[] values = valuesPart.split(",");

            if (values.length != headers.length) {
                throw new SQLSintaxisException();
            }

            StringBuilder row = new StringBuilder();
            for (int i = 0; i < headers.length; i++) {
                row.append(values[i].trim()).append(",");
            }
            row.setLength(row.length() - 1);

            writer.newLine();
            writer.write(row.toString());
            System.out.println("Insertado correctamente.");
        } catch (IOException e) {
            System.err.println("Error al insertar en la tabla: " + e.getMessage());
        }
    }

    // DROP
    public void drop(String entrada){
        String[] querySplit = entrada.trim().split("\\s+");
        if (querySplit.length != 3) {
            System.out.println("Error de sintaxis.");
            return;
        }

        String tableName = querySplit[2].replaceAll(";$", ""); // Eliminar el ';' final si está presente
        Path tablePath = Paths.get(path, tableName + ".csv");

        try {
            if (Files.exists(tablePath)) {
                System.out.print("¿Está seguro de que desea eliminar la tabla " + tableName + "?\n[s/n]: ");
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                String op = br.readLine().toLowerCase();
                if (op.equalsIgnoreCase("s")) {
                    Files.deleteIfExists(tablePath);
                    System.out.println("Tabla eliminada con éxito.");
                }
            } else {
                System.err.println("La tabla indicada no existe.");
            }
        } catch (IOException ignored) {
        }
    }

    // UPDATE
    public void update(String entrada) throws SQLSintaxisException {
        String[] querySplit = entrada.trim().split("\\s+");

        if (querySplit.length < 4 || !querySplit[0].equalsIgnoreCase("update") || !querySplit[2].equalsIgnoreCase("set")) {
            throw new SQLSintaxisException();
        }

        String tableName = querySplit[1];
        List<String> content = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(path + "/" + tableName + ".csv"))) {
            String line;
            while ((line = br.readLine()) != null) {
                content.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (content.isEmpty()) {
            System.err.println("La tabla no existe");
            return;
        }

        String[] setSplit = entrada.substring(entrada.toLowerCase().indexOf("set") + 3).trim().split(",");
        Map<String, String> updates = new HashMap<>();

        for (String set : setSplit) {
            String[] keyValue = set.trim().split("=");
            if (keyValue.length != 2) {
                throw new SQLSintaxisException();
            }
            updates.put(keyValue[0].trim(), keyValue[1].trim());
        }

        List<String> titles = Arrays.asList(content.get(0).split(","));
        List<Integer> updateIndexes = new ArrayList<>();

        for (Map.Entry<String, String> entry : updates.entrySet()) {
            int index = titles.indexOf(entry.getKey());
            if (index == -1) {
                throw new SQLSintaxisException();
            }
            updateIndexes.add(index);
        }

        List<String> updatedContent = new ArrayList<>();
        updatedContent.add(content.get(0)); // Add titles

        for (int i = 1; i < content.size(); i++) {
            String[] row = content.get(i).split(",");
            for (int index : updateIndexes) {
                row[index] = updates.get(titles.get(index));
            }
            updatedContent.add(String.join(",", row));
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path + "/" + tableName + ".csv"))) {
            for (String line : updatedContent) {
                bw.write(line);
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Datos actualizados correctamente");
    }
    // SELECT
    public void select(String entrada) throws IllegalArgumentException, IOException, SQLSintaxisException {
        try {
            String[] querySplit = entrada.trim().split("\\s+");
            String tableName = "";
            List<String> content = new ArrayList<>();

            for (int i = 0; i < querySplit.length; i++) {
                if (querySplit[i].equalsIgnoreCase("from")) {
                    tableName = querySplit[i + 1].replaceAll(";$", "");
                    content = getContent(tableName);
                    break;
                }
            }

            if (content.isEmpty()) {
                System.err.println("La tabla no existe");
            }

            if (entrada.toLowerCase().contains("where")) {
                Where(entrada, querySplit, content);
            } else {
                NoWhere(entrada, querySplit, content);
            }

        } catch (Exception e) {
            throw new SQLSintaxisException();
        }
    }

    private void Where(String query, String[] querySplit, List<String> content) throws SQLSintaxisException {
        int whereIdx = -1;
        for (int i = 0; i < querySplit.length; i++) {
            if (querySplit[i].equalsIgnoreCase("where")) {
                whereIdx = i;
                break;
            }
        }

        if (whereIdx == -1) {
            throw new SQLSintaxisException();
        }

        String condition = query.substring(query.toLowerCase().indexOf("where") + 5);
        String[] conditions = condition.trim().split(" ");

        if (conditions.length != 3) {
            throw new SQLSintaxisException();
        }

        String columnName = conditions[0];
        String operator = conditions[1];
        String value = conditions[2];

        List<String> titles = Arrays.asList(content.get(0).split(","));
        int columnIndex = titles.indexOf(columnName);

        if (columnIndex == -1) {
            throw new SQLSintaxisException();
        }

        for (int i = 1; i < content.size(); i++) {
            String[] row = content.get(i).split(",");
            if (evaluateCondition(row[columnIndex], operator, value)) {
                System.out.println(String.join(",", row));
            }
        }
    }

    private void NoWhere(String query, String[] querySplit, List<String> content) throws SQLSintaxisException {
        if (querySplit[1].equalsIgnoreCase("*")) {
            content.forEach(System.out::println);
        } else {
            List<Integer> columnIndexes = getIntegers(querySplit, content);
            for (int i = 1; i < content.size(); i++) {
                String[] row = content.get(i).split(",");
                List<String> selectedColumns = new ArrayList<>();
                for (int idx : columnIndexes) {
                    selectedColumns.add(row[idx]);
                }
                System.out.println(String.join(",", selectedColumns));
            }
        }
    }

    private static List<Integer> getIntegers(String[] querySplit, List<String> content) throws SQLSintaxisException {
        List<String> titles = Arrays.asList(content.get(0).split(","));
        List<String> columns = new ArrayList<>();
        for (int i = 1; i < querySplit.length; i++) {
            if (!querySplit[i].equalsIgnoreCase("from")) {
                columns.add(querySplit[i].trim());
            } else {
                break;
            }
        }

        List<Integer> columnIndexes = new ArrayList<>();
        for (String column : columns) {
            int index = titles.indexOf(column);
            if (index == -1) {
                throw new SQLSintaxisException();
            }
            columnIndexes.add(index);
        }
        return columnIndexes;
    }

    private boolean evaluateCondition(String value1, String operator, String value2) {
        switch (operator.toLowerCase()) {
            case "=":
                return value1.equals(value2);
            case "<":
                return Integer.parseInt(value1) < Integer.parseInt(value2);
            case ">":
                return Integer.parseInt(value1) > Integer.parseInt(value2);
            default:
                return false;
        }
    }

    private List<String> getContent(String tableName) {
        try {
            Path tablePath = Paths.get(path, tableName + ".csv");
            if (Files.exists(tablePath)) {
                return Files.readAllLines(tablePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }


    //DELETE
    public void delete(String query) throws SQLSintaxisException {
        String[] querySplit = query.split(" ");
        if(querySplit.length > 3){
            if(querySplit[3].equalsIgnoreCase("where")){
                StringBuilder condicion = new StringBuilder();
                for (int i = 4; i < querySplit.length; i++) {
                    condicion.append(querySplit[i]).append(" ");
                }
                List<String> contenido = getContent(querySplit[2]);
                if (!contenido.isEmpty()) {
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(path + "/" + querySplit[2] + "_temp.csv"))) {
                        writer.write(contenido.get(0)); // Escribir encabezados
                        writer.newLine();
                        for (int i = 1; i < contenido.size(); i++) {
                            String fila = contenido.get(i);
                            if (!where(condicion.substring(0, condicion.length() - 1), querySplit[2], fila)) {
                                writer.write(fila);
                                writer.newLine();
                            } else {
                                System.out.println("Contenido Eliminado");
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException("Error al escribir en el archivo temporal.", e);
                    }
                    // Renombrar el archivo temporal al original
                    Path originalFile = Paths.get(path, querySplit[2] + ".csv");
                    Path tempFile = Paths.get(path, querySplit[2] + "_temp.csv");
                    try {
                        Files.move(tempFile, originalFile, StandardCopyOption.REPLACE_EXISTING);
                        System.out.println("Contenido eliminado exitosamente.");
                    } catch (IOException e) {
                        throw new RuntimeException("Error al reemplazar el archivo original.", e);
                    }
                } else {
                    System.err.println("La tabla no existe");
                }
            } else {
                throw new SQLSintaxisException();
            }
        } else {
            throw new SQLSintaxisException();
        }
    }

    private boolean where(String condition, String tableName, String row) {
        // Extrae la columna, el operador y el valor de la condición
        String[] parts = condition.split("\\s+");
        if (parts.length != 3) {
            // La condición es inválida
            return false;
        }
        String columnName = parts[0];
        String operator = parts[1];
        String value = parts[2];

        // Obtén el índice de la columna en la fila
        List<String> titles = Arrays.asList(getContent(tableName).get(0).split(","));
        int columnIndex = titles.indexOf(columnName);

        if (columnIndex == -1) {
            // La columna especificada en la condición no existe en la tabla
            return false;
        }

        // Obtén el valor de la columna en la fila
        String[] rowData = row.split(",");
        if (rowData.length <= columnIndex) {
            // La fila no tiene suficientes columnas
            return false;
        }
        String columnValue = rowData[columnIndex].trim();

        // Evalúa la condición utilizando el operador especificado
        switch (operator) {
            case "=":
                return columnValue.equals(value);
            case "<":
                return columnValue.compareTo(value) < 0;
            case ">":
                return columnValue.compareTo(value) > 0;
            case "<=":
                return columnValue.compareTo(value) <= 0;
            case ">=":
                return columnValue.compareTo(value) >= 0;
            case "!=":
                return !columnValue.equals(value);
            default:
                // Operador no válido
                return false;
        }
    }
}
