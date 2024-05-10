package edu.upvictoria.fpoo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class App {
    public static void main(String[] args) {
        DataManager dbManager = new DataManager();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        try {
            System.out.println("Ingrese consultas SQL (o 'exit' para salir):");
            System.out.print(">");
            String entrada = br.readLine().trim();
            dbManager.procesarEntrada(entrada);

        } catch (IOException e) {
            System.out.println("Error al leer la entrada del usuario: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            System.out.println("Error al establecer la ruta de trabajo: " + e.getMessage());
        } catch (SQLSintaxisException | NoTablesException e) {
            System.out.println("Error en la consulta SQL: " + e.getMessage());
        }
    }
}