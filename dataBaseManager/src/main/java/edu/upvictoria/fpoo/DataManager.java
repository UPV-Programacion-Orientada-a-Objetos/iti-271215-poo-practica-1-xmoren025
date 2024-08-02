package edu.upvictoria.fpoo;


import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataManager {
    public File directory;
    public String path;

    private static final String BASE_DIRECTORY = "C:\\Users\\xmore\\OneDrive\\Escritorio\\iti-271215-poo-practica-1-xmoren025\\dataBaseManager\\src\\main\\java\\edu\\upvictoria\\fpoo";

    public DataManager() {
        path = "";
    }

    public void procesarEntrada(String entrada) throws SQLSintaxisException, NoTablesException, IOException {
        if (entrada.toUpperCase().startsWith("USE")) {
            String[] parts = entrada.split("\\s+");
            if (parts.length != 2) {
                throw new SQLSintaxisException();
            }
            String path = parts[1].trim();
            use(path); // Llama al método use() para establecer la ruta de trabajo
        } else if (this.path.isEmpty()) {
            System.out.println("Debe determinar una ruta de trabajo antes de hacer uso de la base de datos.");
        } else if (entrada.toUpperCase().startsWith("SHOW TABLES")) {
            showTables(directory);
        } else if (entrada.toUpperCase().startsWith("CREATE TABLE")) {
            createTable(entrada);
        } else if (entrada.toUpperCase().startsWith("DROP TABLE")) {
            drop(entrada);
        } else if (entrada.toUpperCase().startsWith("INSERT INTO")) {
            processInsertCommand(entrada);
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

    // USE
    public void use(String directoryName) throws SQLSintaxisException {
        if (!this.path.isEmpty()) {
            System.err.println("La base de datos ya fue seleccionada previamente");
            return;
        }

        if (directoryName.isEmpty()) {
            throw new SQLSintaxisException();
        }

        Path basePath = Paths.get(BASE_DIRECTORY);
        Path targetPath = basePath.resolve(directoryName).normalize();

        if (!targetPath.startsWith(basePath)) {
            System.err.println("La ruta proporcionada no está permitida.");
            this.path = "";
            return;
        }

        this.path = targetPath.toString();
        directory = new File(this.path);

        if (directory.isDirectory()) {
            System.out.println("Base de datos seleccionada exitosamente.");
        } else {
            System.err.println("La base de datos no existe o no es un directorio.");
            this.path = "";
        }
    }

    // CREATE TABLE
    public void createTable(String entrada) throws SQLSintaxisException {
        // Dividir la consulta
        String[] entradaSplit = entrada.trim().split("\\s+");

        if (entradaSplit.length < 3 ||
                !entradaSplit[0].equalsIgnoreCase("CREATE") ||
                !entradaSplit[1].equalsIgnoreCase("TABLE")) {
            // Lanzar excepción con mensaje específico para sintaxis incorrecta
            throw new SQLSintaxisException("La sintaxis de la consulta SQL es incorrecta.");
        }

        // Obtener el nombre de la tabla
        String tableName = entradaSplit[2].trim();
        List<String> columnNames = getColumnNames(entrada);

        if (columnNames.isEmpty()) {
            throw new SQLSintaxisException("No se especificaron nombres de columnas.");
        }

        File tablaFile = new File(path + "/" + tableName + ".csv");
        if (tablaFile.exists()) {
            System.out.println("La tabla ya existe.");
            return;
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tablaFile))) {
            writer.write(String.join(",", columnNames));
            System.out.println("La tabla se creó con éxito.");
        } catch (IOException e) {
            System.err.println("Error al crear la tabla: " + e.getMessage());
        }
    }

    private List<String> getColumnNames(String entrada) throws SQLSintaxisException {
        List<String> columnNames = new ArrayList<>();

        int startColumns = entrada.indexOf("(");
        int endColumns = entrada.lastIndexOf(")");
        if (startColumns == -1 || endColumns == -1 || endColumns < startColumns) {
            throw new SQLSintaxisException();
        }

        String columnsPart = entrada.substring(startColumns + 1, endColumns).trim();
        String[] columnDefinitions = columnsPart.split(",");

        for (String columnDef : columnDefinitions) {
            String columnTrim = columnDef.trim();
            if (!columnTrim.isEmpty()) {
                // Validar y agregar el nombre de la columna
                if (isValidColumnDefinition(columnTrim)) {
                    String columnName = getColumnname(columnTrim);
                    columnNames.add(columnName);
                } else {
                    throw new SQLSintaxisException();
                }
            }
        }
        return columnNames;
    }

    private boolean isValidColumnDefinition(String columnDef) {
        // Validar formato de definición de columna
        return columnDef.matches("^[a-zA-Z0-9_]+\\s+(INT|VARCHAR\\([0-9]+\\))\\s+(NOT NULL|NULL)?(\\s+PRIMARY KEY)?$");
    }

    private String getColumnname(String columnDef) {
        // Extraer el nombre de la columna de la definición de columna
        return columnDef.split("\\s+")[0];
    }

    // SHOW TABLES
    public void showTables(File directory) throws NoTablesException {
        System.out.println("Tablas en la Base de Datos:");
        String[] archivos = directory.list();

        boolean hasTables = false;

        if (archivos != null) {
            for (String archivo : archivos) {
                if (archivo.endsWith(".csv")) {
                    String tableName = archivo.substring(0, archivo.length() - 4);
                    System.out.println(tableName);
                    hasTables = true;
                }
            }

            if (!hasTables) {
                System.out.println("No hay tablas en la base de datos.");
                throw new NoTablesException();
            }
        } else {
            System.out.println("No hay tablas en la base de datos.");
            throw new NoTablesException();
        }
    }

    // INSERT
    private void insert(String tableName, List<String> columns, List<String> values) throws IOException, SQLSintaxisException {
        File file = new File(path + "/" + tableName + ".csv");
        if (!file.exists()) {
            throw new FileNotFoundException("La tabla " + tableName + " no existe.");
        }

        List<String> content = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                content.add(line);
            }
        }


        List<String> titles = Arrays.asList(content.get(0).split(","));
        if (columns.size() != titles.size()) {
            throw new SQLSintaxisException("El número de columnas no coincide con el número de columnas en la tabla.");
        }

        for (String column : columns) {
            if (!titles.contains(column)) {
                throw new SQLSintaxisException("Una o más columnas especificadas no existen en la tabla.");
            }
        }

        if (values.size() != columns.size()) {
            throw new SQLSintaxisException("El número de valores no coincide con el número de columnas.");
        }

        StringBuilder newRow = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) newRow.append(",");
            newRow.append(values.get(i));
        }
        content.add(newRow.toString());

        // Guardar el contenido actualizado en el archivo
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            for (String line : content) {
                bw.write(line);
                bw.newLine();
            }
        }
    }

    // Método para procesar el comando INSERT INTO
    public void processInsertCommand(String command) throws IOException, SQLSintaxisException {
        command = command.trim().toLowerCase();
        if (!command.startsWith("insert into")) {
            throw new SQLSintaxisException("El comando debe comenzar con 'INSERT INTO'.");
        }

        command = command.substring("insert into".length()).trim();

        int valuesIdx = command.indexOf("values");
        if (valuesIdx == -1) {
            throw new SQLSintaxisException("Falta la cláusula 'VALUES' en el comando.");
        }

        String tablePart = command.substring(0, valuesIdx).trim();
        String valuesPart = command.substring(valuesIdx + "values".length()).trim();


        int parenIdx = tablePart.indexOf('(');
        if (parenIdx == -1) {
            throw new SQLSintaxisException("Falta el paréntesis de apertura en el nombre de la tabla y columnas.");
        }

        String tableName = tablePart.substring(0, parenIdx).trim();
        String columnsPart = tablePart.substring(parenIdx + 1, tablePart.indexOf(')')).trim();

        if (columnsPart.isEmpty()) {
            throw new SQLSintaxisException("Las columnas no pueden estar vacías.");
        }
        List<String> columns = Arrays.asList(columnsPart.split("\\s*,\\s*"));


        if (!valuesPart.startsWith("(") || !valuesPart.endsWith(")")) {
            throw new SQLSintaxisException("Los valores deben estar entre paréntesis.");
        }

        String valuesString = valuesPart.substring(1, valuesPart.length() - 1).trim();
        List<String> values = Arrays.asList(valuesString.split("\\s*,\\s*"));


        if (columns.size() != values.size()) {
            throw new SQLSintaxisException("El número de columnas no coincide con el número de valores.");
        }


        try {
            insert(tableName, columns, values);
            System.out.println("Se insertaron los datos con éxito.");
        } catch (SQLSintaxisException e) {
            throw new SQLSintaxisException("Error al insertar datos: " + e.getMessage());
        }
    }

    // DROP
    public void drop(String entrada) {
        String[] entradaSplit = entrada.trim().split("\\s+");
        if (entradaSplit.length != 3) {
            System.out.println("Error de sintaxis.");
            return;
        }

        String tableName = entradaSplit[2].replaceAll(";$", ""); // Eliminar el ';' final si está presente
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
    public void update(String entrada) throws IllegalArgumentException, IOException, SQLSintaxisException {
        try {
            String[] entradaSplit = entrada.trim().split("\\s+");
            String tableName = "";
            List<String> content = new ArrayList<>();

            // Obtener el nombre de la tabla
            for (int i = 0; i < entradaSplit.length; i++) {
                if (entradaSplit[i].equalsIgnoreCase("update")) {
                    tableName = entradaSplit[i + 1].replaceAll(";$", "");
                    content = getContent(tableName);
                    break;
                }
            }

            if (content.isEmpty()) {
                System.err.println("La tabla no existe");
                return;
            }

            // Encontrar la posición de la cláusula SET y WHERE
            int setIdx = entrada.toLowerCase().indexOf("set");
            int whereIdx = entrada.toLowerCase().indexOf("where");

            if (setIdx == -1 || whereIdx == -1) {
                throw new SQLSintaxisException("Error en la consulta SQL: La cláusula 'SET' o 'WHERE' está en un formato incorrecto.");
            }

            // Extraer y procesar las partes de la consulta
            String setClause = entrada.substring(setIdx + 3, whereIdx).trim();
            String conditionClause = entrada.substring(whereIdx + 5).trim();

            // Procesar la cláusula SET
            String[] setPairs = setClause.split(",");
            Map<String, String> updates = new HashMap<>();
            for (String pair : setPairs) {
                String[] keyValue = pair.trim().split("=");
                if (keyValue.length != 2) {
                    throw new SQLSintaxisException("Error en la consulta SQL: La cláusula 'SET' tiene un formato incorrecto.");
                }
                String column = keyValue[0].trim();
                String value = keyValue[1].trim().replaceAll("^'(.*)'$", "$1"); // Elimina comillas simples
                updates.put(column, value);
            }

            // Procesar la cláusula WHERE
            String[] conditionParts = conditionClause.split("\\s+");
            if (conditionParts.length != 3) {
                throw new SQLSintaxisException("Error en la consulta SQL: La cláusula 'WHERE' tiene un formato incorrecto.");
            }

            String conditionColumn = conditionParts[0];
            String conditionOperator = conditionParts[1];
            String conditionValue = conditionParts[2].replaceAll("^'(.*)'$", "$1"); // Elimina comillas simples

            // Obtener el índice de la columna de la tabla
            List<String> titles = Arrays.asList(content.get(0).split(","));
            int conditionIndex = titles.indexOf(conditionColumn);

            if (conditionIndex == -1) {
                throw new SQLSintaxisException("Error en la consulta SQL: Columna en la cláusula 'WHERE' no encontrada.");
            }

            // Actualizar las filas
            boolean updated = false;
            for (int i = 1; i < content.size(); i++) {
                String[] row = content.get(i).split(",");
                if (evaluateCondition(row[conditionIndex], conditionOperator, conditionValue)) {
                    updated = true;
                    // Actualizar valores
                    for (Map.Entry<String, String> entry : updates.entrySet()) {
                        int columnIndex = titles.indexOf(entry.getKey());
                        if (columnIndex != -1) {
                            row[columnIndex] = entry.getValue();
                        }
                    }
                    // Reemplazar fila en el contenido
                    content.set(i, String.join(",", row));
                }
            }

            if (!updated) {
                System.out.println("No se encontraron filas que coincidan con la condición.");
            } else {
                // Guardar los cambios en el archivo
                try {
                    Files.write(Paths.get(path, tableName + ".csv"), content);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.err.println("Error al guardar los cambios.");
                }
            }

        } catch (Exception e) {
            throw new SQLSintaxisException("Error en la consulta SQL: " + e.getMessage());
        }
    }

    // SELECT
    public void select(String entrada) throws IllegalArgumentException, IOException, SQLSintaxisException {
        try {
            String[] entradaSplit = entrada.trim().split("\\s+");
            String tableName = "";
            List<String> content = new ArrayList<>();

            for (int i = 0; i < entradaSplit.length; i++) {
                if (entradaSplit[i].equalsIgnoreCase("from")) {
                    tableName = entradaSplit[i + 1].replaceAll(";$", "");
                    content = getContent(tableName);
                    break;
                }
            }

            if (content.isEmpty()) {
                System.err.println("La tabla no existe");
                return;
            }

            if (entrada.toLowerCase().contains("where")) {
                where(entrada, entradaSplit, content);
            } else {
                NoWhere(entradaSplit, content);
            }

        } catch (Exception e) {
            throw new SQLSintaxisException("Error en la consulta SQL: " + e.getMessage());
        }
    }

    private void where(String entrada, String[] entradaSplit, List<String> content) throws SQLSintaxisException {
        int whereIdx = -1;
        for (int i = 0; i < entradaSplit.length; i++) {
            if (entradaSplit[i].equalsIgnoreCase("where")) {
                whereIdx = i;
                break;
            }
        }

        if (whereIdx == -1) {
            throw new SQLSintaxisException("No se encontró la cláusula WHERE.");
        }

        String condition = entrada.substring(entrada.toLowerCase().indexOf("where") + 5).trim();
        String[] conditions = condition.split("\\s+");

        if (conditions.length != 3) {
            throw new SQLSintaxisException("La condición WHERE no es válida.");
        }

        String columnName = conditions[0];
        String operator = conditions[1];
        String value = conditions[2].replaceAll("^'(.*)'$", "$1"); // Elimina comillas simples

        List<String> titles = Arrays.asList(content.get(0).split(","));
        int columnIndex = titles.indexOf(columnName);

        if (columnIndex == -1) {
            throw new SQLSintaxisException("Columna no encontrada: " + columnName);
        }

        for (int i = 1; i < content.size(); i++) {
            String[] row = content.get(i).split(",");
            if (evaluateCondition(row[columnIndex], operator, value)) {
                System.out.println(String.join(",", row));
            }
        }
    }

    private void NoWhere(String[] entradaSplit, List<String> content) throws SQLSintaxisException {
        if (entradaSplit[1].equalsIgnoreCase("*")) {
            content.forEach(System.out::println);
        } else {
            List<Integer> columnIndexes = getIntegers(entradaSplit, content);
            for (int i = 1; i < content.size(); i++) {
                String[] row = content.get(i).split(",");
                List<String> selectedColumns = new ArrayList<>();
                for (int idx : columnIndexes) {
                    if (idx >= 0 && idx < row.length) {
                        selectedColumns.add(row[idx]);
                    }
                }
                System.out.println(String.join(",", selectedColumns));
            }
        }
    }

    private List<Integer> getIntegers(String[] entradaSplit, List<String> content) throws SQLSintaxisException {
        List<Integer> columnIndexes = new ArrayList<>();
        String[] columns = entradaSplit[1].split(",");
        List<String> titles = Arrays.asList(content.get(0).split(","));
        for (String column : columns) {
            int index = titles.indexOf(column);
            if (index == -1) {
                throw new SQLSintaxisException("Columna no encontrada: " + column);
            }
            columnIndexes.add(index);
        }
        return columnIndexes;
    }

    private boolean evaluateCondition(String columnValue, String operator, String value) {
        return switch (operator) {
            case "=" -> columnValue.equals(value);
            case "!=" -> !columnValue.equals(value);
            case "<" -> Double.parseDouble(columnValue) < Double.parseDouble(value);
            case ">" -> Double.parseDouble(columnValue) > Double.parseDouble(value);
            default -> false;
        };
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

    // DELETE
    public void delete(String entrada) throws SQLSintaxisException {
        String[] entradaSplit = entrada.trim().split("\\s+");

        // Verificar la sintaxis del comando
        if (entradaSplit.length < 4 || !entradaSplit[0].equalsIgnoreCase("DELETE")
                || !entradaSplit[1].equalsIgnoreCase("FROM")) {
            throw new SQLSintaxisException();
        }

        // Obtener el nombre de la tabla
        String tableName = entradaSplit[2];

        // Verificar si existe la sección WHERE
        if (entradaSplit.length > 4 && entradaSplit[3].equalsIgnoreCase("WHERE")) {
            StringBuilder condicion = new StringBuilder();
            for (int i = 4; i < entradaSplit.length; i++) {
                condicion.append(entradaSplit[i]).append(" ");
            }

            // Limpiar la condición
            String condition = condicion.toString().trim();

            // Obtener el contenido de la tabla
            List<String> contenido = getContent(tableName);
            if (contenido.isEmpty()) {
                System.err.println("La tabla no existe o está vacía.");
                return;
            }

            // Crear archivo temporal
            File tempFile = new File(path + "/" + tableName + "_temp.csv");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
                writer.write(contenido.get(0)); // Escribir encabezados
                writer.newLine();

                // Evaluar y escribir las filas que no cumplen con la condición
                boolean filasEliminadas = false;
                for (int i = 1; i < contenido.size(); i++) {
                    String fila = contenido.get(i);
                    if (!evaluateCondition(contenido.get(0).split(","), fila.split(","), condition)) {
                        writer.write(fila);
                        writer.newLine();
                    } else {
                        System.out.println("Fila eliminada: " + fila);
                        filasEliminadas = true;
                    }
                }

                if (!filasEliminadas) {
                    System.out.println("No se eliminaron filas que cumplan con la condición.");
                }
            } catch (IOException e) {
                throw new RuntimeException("Error al escribir en el archivo temporal.", e);
            }

            // Reemplazar el archivo original con el archivo temporal
            Path originalFile = Paths.get(path, tableName + ".csv");
            try {
                Files.move(tempFile.toPath(), originalFile, StandardCopyOption.REPLACE_EXISTING);
                System.out.println("Contenido eliminado exitosamente.");
            } catch (IOException e) {
                throw new RuntimeException("Error al reemplazar el archivo original.", e);
            }
        } else {
            // Sin sección WHERE, eliminar todas las filas
            File originalFile = new File(path + "/" + tableName + ".csv");
            if (!originalFile.delete()) {
                throw new RuntimeException("Error al eliminar el archivo original.");
            }
            System.out.println("Todas las filas eliminadas.");
        }
    }


    // Evaluar la condición para una fila específica
    private boolean evaluateCondition(String[] header, String[] row, String condition) {
        Map<String, String> columnValueMap = new HashMap<>();
        for (int i = 0; i < header.length; i++) {
            columnValueMap.put(header[i].trim(), row[i].trim());
        }

        String[] orConditions = condition.split("\\s+OR\\s+");
        for (String orCondition : orConditions) {
            String[] andConditions = orCondition.split("\\s+AND\\s+");
            boolean match = true;
            for (String andCondition : andConditions) {
                match = evaluateSingleCondition(columnValueMap, andCondition.trim());
                if (!match) break;
            }
            if (match) return true;
        }
        return false;
    }

    // Evaluar una sola condición
    private boolean evaluateSingleCondition(Map<String, String> columnValueMap, String condition) {
        // Expresión regular para parsear la condición
        Pattern pattern = Pattern.compile("(\\w+)\\s*(=|<>|<|>|<=|>=)\\s*'?([^']*)'?");
        Matcher matcher = pattern.matcher(condition);
        if (!matcher.matches()) {
            return false;
        }

        String column = matcher.group(1);
        String operator = matcher.group(2);
        String value = matcher.group(3).replace("'", ""); // Quitar comillas simples

        String columnValue = columnValueMap.get(column);
        if (columnValue == null) return false;

        // Evaluar la condición según el operador
        return switch (operator) {
            case "=" -> columnValue.equals(value);
            case "<>" -> !columnValue.equals(value);
            case "<" -> Integer.parseInt(columnValue) < Integer.parseInt(value);
            case ">" -> Integer.parseInt(columnValue) > Integer.parseInt(value);
            case "<=" -> Integer.parseInt(columnValue) <= Integer.parseInt(value);
            case ">=" -> Integer.parseInt(columnValue) >= Integer.parseInt(value);
            default -> false;
        };
    }
}