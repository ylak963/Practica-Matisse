package es.studium.MatisseBD;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.matisse.MtDatabase;
import com.matisse.MtException;
import com.matisse.MtObjectIterator;
import com.matisse.MtPackageObjectFactory;

import academia.Clases;
import academia.Cursos;
import academia.Profesores;


public class GestionDB 
{
	public static void main(String[] args)
	{
		String hostname = "localhost";
		String dbname = "academia";
		/* Establece la conexión con la BD creada en Matisse */
		// conectaralaBD(hostname,dbname);

		// USADO PARA: Crear un objeto en la base de datos
		//crearObjetos(hostname, dbname);

		/* Borra todos los objetos de una clase */
		bajaObjetos(hostname,dbname);

		/* Modificar objetos */
		//modificarObjetos(hostname,dbname,"Maria",954654321);

		/* Consulta OQL con JDBC */
		//consultaObjetos(hostname,dbname);
	}
	// Conectarse a la base de datos academia creada en Matisse
	public static  void crearObjetos(String hostname, String dbname)
	{
		try
		{
			// Abre la base de datos con el Hostname (localhost),dbname (academia) y el namespace "academia".
			MtDatabase db = new MtDatabase(hostname, dbname, new MtPackageObjectFactory("", " academia"));
			db.open();
			db.startTransaction();
			System.out.println("Conectado a la base de datos " + db.toString() + " de Matisse");

			// Crea un objeto Profesores
			Profesores pr1 = new Profesores(db);
			pr1.setNombre("Pepe");
			pr1.setApellidos("Diaz");
			pr1.setTelefono(954123456);
			pr1.setDni(12345689);
			System.out.println("Profesor 'Informatica' creado...");

			// Crea un objeto Clases
			Clases cl1 = new Clases(db);
			cl1.setNombre("Informatica");
			cl1.setAula("a");
			cl1.setDuracion(1);
			cl1.setHoraInicio(5);

			// Crea un objeto Cursos
			Cursos cur1 = new Cursos(db);
			cur1.setFecha("2020,2,31");
			
			//Crear otro objetos Cursos
			Cursos cur2 = new Cursos(db);
			cur2.setFecha("2020,5,10");

			System.out.println("El curso empieza en la fecha 'tal fecha' creado...");

			// Crea un array de Clases para guardar los cursos y hacer las relaciones
			Clases cl11[] = new Clases[2];
			cl11[0] = cur1;
			cl11[1] = cur2;

			// Guarda las relaciones del profesor con las clases que ha impartido.
			pr1.setImparten(cl11);
			// Ejecuta un commit para materializar las peticiones.
			db.commit();
			// Cierra la base de datos.
			db.close();
			System.out.println("\nHecho.");
		} 
		catch (MtException mte) 
		{
			System.out.println("MtException : " + mte.getMessage());
		}
	}
	// Borrar todos los objetos de una clase
	public static void bajaObjetos(String hostname, String dbname) 
	{
		System.out.println("====================== Borrar Todos	=====================\n");
		try 
		{
			MtDatabase db = new MtDatabase(hostname, dbname, new MtPackageObjectFactory("", "academia"));
			db.open();
			db.startTransaction();
			// Lista todos los objetos CLASES que hay en la base de datos, con el método
			// getInstanceNumber
			System.out.println("\n" + Clases.getInstanceNumber(db) + "Clase(s) en la DB.");
			// Borra todas las instancias de Clases
			Clases.getClass(db).removeAllInstances();
			//Clases.getClass(db).remove();
			// materializa los cambios y cierra la BD
			db.commit();
			db.close();
			System.out.println("\nHecho.");
		} 
		catch (MtException mte) 
		{
			System.out.println("MtException : " + mte.getMessage());
		}
	}
	public static void consultaObjetos(String hostname, String dbname) 
	{
		MtDatabase dbcon = new MtDatabase(hostname, dbname);
		// Abre una conexión a la base de datos
		dbcon.open();
		//Se crea la conexion JDBC
		Connection jdbcon = dbcon.getJDBCConnection();
		
		try 
		{
			
			// Crea una instancia de Statement
			Statement stmt = dbcon.createStatement();
			// Asigna una consulta OQL. Esta consulta lo que hace es utilizar REF() para obtener el objeto
			// directamente en vez de obtener valores concretos (que también podría ser).
			String commandText = "SELECT REF(p) from academia.Profesores p;";
			// Ejecuta la consulta y obtiene un ResultSet
			ResultSet rset = stmt.executeQuery(commandText);
			Profesores pr1 =null; //Revisar
			// Lee rset uno a uno.
			while (rset.next()) 
			{
				// Obtiene los objetos Profesores.
				pr1 = (Profesores) rset.getObject(1);
				// Imprime los atributos de cada objeto con un formato determinado.
			}
			System.out.println("Profesores: " +	String.format("%16s", pr1.getNombre())
			+ String.format("%16s",	pr1.getApellidos()) + " Contacto: " + String.format("%16s", pr1.getTelefono())+ " con Dni: " + String.format("%16s",pr1.getDni()));
			// Cierra las conexiones
			rset.close();
			stmt.close();
		}

		catch(SQLException e) 
		{
			System.out.println("SQLException: " + e.getMessage());
		}
	} 
	public static void modificarObjetos(String hostname, String dbname, String nombre, Integer telefono) 
	{
		System.out.println("=========== Modifica un objeto==========\n");
		int nNombre = 0;
		try 
		{
			MtDatabase db = new MtDatabase(hostname, dbname, new
					MtPackageObjectFactory("", "academia"));
			db.open();
			db.startTransaction();
			// Lista cuántos objetos Profesores con el método
			//getInstanceNumber
			System.out.println("\n" + Profesores.getInstanceNumber(db) + "Profesores en la DB.");
			nNombre = (int) Profesores.getInstanceNumber(db);
			// Crea un Iterador (propio de Java)
			MtObjectIterator<Profesores> iter = Profesores.<Profesores>instanceIterator(db);
			System.out.println("recorro el iterador de uno en uno y	cambio cuando encuentro 'nombre'");
			while (iter.hasNext()) 
			{
				Profesores[] profesores = iter.next(nNombre);
				for (int i = 0; i < profesores.length; i++)
				{
					if(profesores[i].getNombre().equalsIgnoreCase(nombre))
					{
						profesores[i].setTelefono(telefono);
					}
					else
					{
						
					}
				}
				break;
			}
		
		iter.close();
		// materializa los cambios y cierra la BD
		db.commit();
		db.close();
		System.out.println("\nHEcho.");
	} 
	catch (MtException mte) 
	{
		System.out.println("MtException : " + mte.getMessage());
	}
}
}
