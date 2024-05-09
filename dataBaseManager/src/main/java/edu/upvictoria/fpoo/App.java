package edu.upvictoria.fpoo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class App {
    public static void main(String[] args) {
        DataManager dbManager = new DataManager();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        try {

            //("/home/xmoreno/iti-271215-poo-practica-1-xmoren025/dataBaseManager/src/main/java/edu/upvictoria/fpoo/$PATH$");
            System.out.println("Ingrese el comando << USE $PATH$ >> para ingresar a la base de datos:");
            String entrada = br.readLine().trim();
            if (entrada.toUpperCase().startsWith("USE")) {
                dbManager.use(entrada);
                while(true){
                    System.out.println("Ingrese una consulta SQL (o 'exit' para salir:)");
                    entrada = br.readLine().trim();

                    if (entrada.equalsIgnoreCase("exit")){
                        break;
                    }

                    //Identificar el comando



                }
            }



        } catch(IOException e){
            System.out.println("Error al leer la entrada del usuario:" +e.getMessage());

        }catch (IllegalArgumentException e) {
            System.out.println("Error al establecer la ruta de trabajo: " + e.getMessage());
        }

    }
}


