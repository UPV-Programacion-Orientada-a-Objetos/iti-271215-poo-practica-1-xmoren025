package edu.upvictoria.fpoo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class App {
    public static void main(String[] args) {
        DataManager dbManager = new DataManager();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Ingrese consultas SQL (o 'exit' para salir):");
        try {
            String entrada;
            do {
                entrada = readInput();
                dbManager.procesarEntrada(entrada);
            } while (!entrada.equalsIgnoreCase("exit"));
        } catch (IOException e) {
            System.out.println("Error al leer la entrada del usuario: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            System.out.println("Error al establecer la ruta de trabajo: " + e.getMessage());
        } catch (SQLSintaxisException | NoTablesException e) {
            System.out.println("Error en la consulta SQL: " + e.getMessage());
        }
    }

    private static String readInput() throws IOException {
        System.out.print(">");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        return br.readLine().trim();
    }
}