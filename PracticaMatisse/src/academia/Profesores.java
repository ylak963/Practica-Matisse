/*
 * Profesores.java
 *
 * Generated with Matisse Schema Definition Language 9.1.11
 * Generation date: Sun Jan 17 01:47:11 2021
 */

// Note: the package and extends declarations are generated by mt_sdl, do not modify them

package academia;

import com.matisse.reflect.*;

/**
 * <code>Profesores</code> is a schema class generated by <code>mt_sdl</code>.
 * Any user-written classes will be found at the end of the file, after
 * the '// END of Matisse SDL Generated Code' comment.
 * Attribute types, default values, and relationship minimum and maximum
 * cardinality are stored in the database itself, not in this source code.
 * For more information, see <i>Getting Started with MATISSE</i>.
 */
public class Profesores extends com.matisse.reflect.MtObject {

    // BEGIN Matisse SDL Generated Code
    // DO NOT MODIFY UNTIL THE 'END of Matisse SDL Generated Code' MARK BELOW
    /*
     * Generated with Matisse Schema Definition Language 9.1.11
     * Generation Date: Sun Jan 17 02:20:37 2021
     */

    /*
     * Class variables and methods
     */

    /** Class <code>Profesores</code> cache ID */
    private static int CID = com.matisse.MtDatabase.allocCID(new com.matisse.reflect.MtClass.Loader("academia.Profesores"));

    /**
     * Gets the <code>Profesores</code> class descriptor.
     * This method supports advanced MATISSE programming techniques such as
     * dynamically modifying the schema.
     * @param db a database
     * @return a class descriptor
     */
    public static com.matisse.reflect.MtClass getClass(com.matisse.MtDatabase db) {
        return (com.matisse.reflect.MtClass)db.getCachedObject(CID);
    }

    /**
     * Factory constructor. This constructor is called by <code>MtObjectFactory</code>.
     * It is public for technical reasons but is not intended to be called
     * directly by user methods.
     * @param db a database
     * @param mtOid an existing object ID in the database
     */
    public Profesores(com.matisse.MtDatabase db, int mtOid)  {
        super(db, mtOid);
    }

    /**
     * Cascaded constructor, used by subclasses to create a new object in the database.
     * It is protected for technical reasons but is not intended to be called
     * directly by user methods.
     * @param mtCls a class descriptor (the class to instantiate)
     */
    protected Profesores(com.matisse.reflect.MtClass mtCls)  {
        super(mtCls);
    }

    /**
     * Opens an iterator on all instances of this class (and its subclasses).
     * @param <E> a MtObject class     * @param db a database
     * @return an object iterator
     */
    public static <E extends com.matisse.reflect.MtObject> com.matisse.MtObjectIterator<E> instanceIterator(com.matisse.MtDatabase db) {
        return getClass(db).<E>instanceIterator(Profesores.class);
    }

    /**
     * Opens an iterator on all instances of this class (and its subclasses).
     * @param <E> a MtObject class     * @param db a database
     * @param numObjPerBuffer maximum number of objects to fetch from the server at a time
     * @return an object iterator
     */
    public static <E extends com.matisse.reflect.MtObject> com.matisse.MtObjectIterator<E> instanceIterator(com.matisse.MtDatabase db, int numObjPerBuffer) {
        return getClass(db).<E>instanceIterator(numObjPerBuffer, Profesores.class);
    }

    /**
     * Counts the number of instances of this class (and its subclasses).
     * @param db a database
     * @return total number of instances
     */
    public static long getInstanceNumber(com.matisse.MtDatabase db) {
        return getClass(db).getInstanceNumber();
    }

    /**
     * Opens an iterator on all own instances of this class (excluding subclasses).
     * @param <E> a MtObject class     * @param db a database
     * @return an object iterator
     */
    public static <E extends com.matisse.reflect.MtObject> com.matisse.MtObjectIterator<E> ownInstanceIterator(com.matisse.MtDatabase db) {
        return getClass(db).<E>ownInstanceIterator(Profesores.class);
    }

    /**
     * Opens an iterator on all own instances of this class (excluding subclasses).
     * @param <E> a MtObject class     * @param db a database
     * @param numObjPerBuffer maximum number of objects to fetch from the server at a time
     * @return an object iterator
     */
    public static <E extends com.matisse.reflect.MtObject> com.matisse.MtObjectIterator<E> ownInstanceIterator(com.matisse.MtDatabase db, int numObjPerBuffer) {
        return getClass(db).<E>ownInstanceIterator(numObjPerBuffer, Profesores.class);
    }

    /**
     * Counts the number of own instances of this class (excluding subclasses).
     * @param db a database
     * @return total number of instances
     */
    public static long getOwnInstanceNumber(com.matisse.MtDatabase db) {
        return getClass(db).getOwnInstanceNumber();
    }

    /*
     * Attribute access methods
     */

    /* Attribute 'nombre' */

    /** Attribute <code>nombre</code> cache ID */
    private static int nombreCID = com.matisse.MtDatabase.allocCID(new com.matisse.reflect.MtAttribute.Loader("nombre",CID));

    /**
     * Gets the <code>nombre</code> attribute descriptor.
     * This method supports advanced MATISSE programming techniques such as
     * dynamically modifying the schema.
     * @param db the database containing the attribute
     * @return the attribute descriptor object
     */
    public static com.matisse.reflect.MtAttribute getNombreAttribute(com.matisse.MtDatabase db) {
        return (com.matisse.reflect.MtAttribute)db.getCachedObject(nombreCID);
    }


    /**
     * Gets the <code>nombre</code> attribute value.
     * @return the value
     *
     * @see #setNombre
     * @see #removeNombre
     */
    public final String getNombre() {
        return getString(getNombreAttribute(getMtDatabase()));
    }

    /**
     * Sets the <code>nombre</code> attribute value.
     * @param val the new value
     *
     * @see #getNombre
     * @see #removeNombre
     */
    public final void setNombre(String val) {
        setString(getNombreAttribute(getMtDatabase()), val);
    }

    /**
     * Removes the current <code>nombre</code> attribute value.
     * Falls back to the default value is there is one.  If the attribute
     * has no default value, it must be set to a new value before commit.
     *
     * @see #getNombre
     * @see #setNombre
     */
    public final void removeNombre() {
        removeValue(getNombreAttribute(getMtDatabase()));
    }

    /**
     * Check if nullable attribute value is set to MT_NULL.
     * @return true if null value
     *
     * @see #getNombre
     * @see #setNombre
     */
    public final boolean isNombreNull() {
        return isNull(getNombreAttribute(getMtDatabase()));
    }

    /**
     * Check if attribute value is set to its default value.
     * @return true if default value
     *
     * @see #getNombre
     * @see #setNombre
     */
    public final boolean isNombreDefaultValue() {
        return isDefaultValue(getNombreAttribute(getMtDatabase()));
    }


    /* Attribute 'apellidos' */

    /** Attribute <code>apellidos</code> cache ID */
    private static int apellidosCID = com.matisse.MtDatabase.allocCID(new com.matisse.reflect.MtAttribute.Loader("apellidos",CID));

    /**
     * Gets the <code>apellidos</code> attribute descriptor.
     * This method supports advanced MATISSE programming techniques such as
     * dynamically modifying the schema.
     * @param db the database containing the attribute
     * @return the attribute descriptor object
     */
    public static com.matisse.reflect.MtAttribute getApellidosAttribute(com.matisse.MtDatabase db) {
        return (com.matisse.reflect.MtAttribute)db.getCachedObject(apellidosCID);
    }


    /**
     * Gets the <code>apellidos</code> attribute value.
     * @return the value
     *
     * @see #setApellidos
     * @see #removeApellidos
     */
    public final String getApellidos() {
        return getString(getApellidosAttribute(getMtDatabase()));
    }

    /**
     * Sets the <code>apellidos</code> attribute value.
     * @param val the new value
     *
     * @see #getApellidos
     * @see #removeApellidos
     */
    public final void setApellidos(String val) {
        setString(getApellidosAttribute(getMtDatabase()), val);
    }

    /**
     * Removes the current <code>apellidos</code> attribute value.
     * Falls back to the default value is there is one.  If the attribute
     * has no default value, it must be set to a new value before commit.
     *
     * @see #getApellidos
     * @see #setApellidos
     */
    public final void removeApellidos() {
        removeValue(getApellidosAttribute(getMtDatabase()));
    }

    /**
     * Check if nullable attribute value is set to MT_NULL.
     * @return true if null value
     *
     * @see #getApellidos
     * @see #setApellidos
     */
    public final boolean isApellidosNull() {
        return isNull(getApellidosAttribute(getMtDatabase()));
    }

    /**
     * Check if attribute value is set to its default value.
     * @return true if default value
     *
     * @see #getApellidos
     * @see #setApellidos
     */
    public final boolean isApellidosDefaultValue() {
        return isDefaultValue(getApellidosAttribute(getMtDatabase()));
    }


    /* Attribute 'telefono' */

    /** Attribute <code>telefono</code> cache ID */
    private static int telefonoCID = com.matisse.MtDatabase.allocCID(new com.matisse.reflect.MtAttribute.Loader("telefono",CID));

    /**
     * Gets the <code>telefono</code> attribute descriptor.
     * This method supports advanced MATISSE programming techniques such as
     * dynamically modifying the schema.
     * @param db the database containing the attribute
     * @return the attribute descriptor object
     */
    public static com.matisse.reflect.MtAttribute getTelefonoAttribute(com.matisse.MtDatabase db) {
        return (com.matisse.reflect.MtAttribute)db.getCachedObject(telefonoCID);
    }


    /**
     * Gets the <code>telefono</code> attribute value.
     * @return the value
     *
     * @see #setTelefono
     * @see #removeTelefono
     */
    public final int getTelefono() {
        return getInteger(getTelefonoAttribute(getMtDatabase()));
    }

    /**
     * Sets the <code>telefono</code> attribute value.
     * @param val the new value
     *
     * @see #getTelefono
     * @see #removeTelefono
     */
    public final void setTelefono(int val) {
        setInteger(getTelefonoAttribute(getMtDatabase()), val);
    }

    /**
     * Removes the current <code>telefono</code> attribute value.
     * Falls back to the default value is there is one.  If the attribute
     * has no default value, it must be set to a new value before commit.
     *
     * @see #getTelefono
     * @see #setTelefono
     */
    public final void removeTelefono() {
        removeValue(getTelefonoAttribute(getMtDatabase()));
    }

    /**
     * Check if nullable attribute value is set to MT_NULL.
     * @return true if null value
     *
     * @see #getTelefono
     * @see #setTelefono
     */
    public final boolean isTelefonoNull() {
        return isNull(getTelefonoAttribute(getMtDatabase()));
    }

    /**
     * Check if attribute value is set to its default value.
     * @return true if default value
     *
     * @see #getTelefono
     * @see #setTelefono
     */
    public final boolean isTelefonoDefaultValue() {
        return isDefaultValue(getTelefonoAttribute(getMtDatabase()));
    }


    /* Attribute 'dni' */

    /** Attribute <code>dni</code> cache ID */
    private static int dniCID = com.matisse.MtDatabase.allocCID(new com.matisse.reflect.MtAttribute.Loader("dni",CID));

    /**
     * Gets the <code>dni</code> attribute descriptor.
     * This method supports advanced MATISSE programming techniques such as
     * dynamically modifying the schema.
     * @param db the database containing the attribute
     * @return the attribute descriptor object
     */
    public static com.matisse.reflect.MtAttribute getDniAttribute(com.matisse.MtDatabase db) {
        return (com.matisse.reflect.MtAttribute)db.getCachedObject(dniCID);
    }


    /**
     * Gets the <code>dni</code> attribute value.
     * @return the value
     *
     * @see #setDni
     * @see #removeDni
     */
    public final int getDni() {
        return getInteger(getDniAttribute(getMtDatabase()));
    }

    /**
     * Sets the <code>dni</code> attribute value.
     * @param val the new value
     *
     * @see #getDni
     * @see #removeDni
     */
    public final void setDni(int val) {
        setInteger(getDniAttribute(getMtDatabase()), val);
    }

    /**
     * Removes the current <code>dni</code> attribute value.
     * Falls back to the default value is there is one.  If the attribute
     * has no default value, it must be set to a new value before commit.
     *
     * @see #getDni
     * @see #setDni
     */
    public final void removeDni() {
        removeValue(getDniAttribute(getMtDatabase()));
    }

    /**
     * Check if nullable attribute value is set to MT_NULL.
     * @return true if null value
     *
     * @see #getDni
     * @see #setDni
     */
    public final boolean isDniNull() {
        return isNull(getDniAttribute(getMtDatabase()));
    }

    /**
     * Check if attribute value is set to its default value.
     * @return true if default value
     *
     * @see #getDni
     * @see #setDni
     */
    public final boolean isDniDefaultValue() {
        return isDefaultValue(getDniAttribute(getMtDatabase()));
    }


    /*
     * Relationship access methods
     */

    /* Relationship 'imparten' */

    /** Relationship <code>imparten</code> cache ID */
    private static int impartenCID = com.matisse.MtDatabase.allocCID(new com.matisse.reflect.MtRelationship.Loader("imparten",CID));

    /**
     * Gets the <code>imparten</code> relationship descriptor.
     * This method supports advanced MATISSE programming techniques such as
     * dynamically modifying the schema.
     * @param db a database
     * @return a relationship descriptor object
     */
    public static com.matisse.reflect.MtRelationship getImpartenRelationship(com.matisse.MtDatabase db) {
        return (com.matisse.reflect.MtRelationship)db.getCachedObject(impartenCID);
    }

    /**
     * Gets the <code>imparten</code> relationship's successors.
     * @return an array of objects
     *
     * @see #getImpartenSize
     * @see #impartenIterator
     * @see #setImparten
     * @see #removeImparten
     */
    public final academia.Clases[] getImparten() {
        return (academia.Clases[])getSuccessors(getImpartenRelationship(getMtDatabase()), academia.Clases.class);
    }

    /**
     * Counts the <code>imparten</code> relationship's successors.
     * @return the number of successors
     *
     * @see #getImparten
     * @see #impartenIterator
     * @see #setImparten
     * @see #removeImparten
     */
    public final int getImpartenSize() {
        return getSuccessorSize(getImpartenRelationship(getMtDatabase()));
    }

    /**
     * Opens an iterator on the <code>imparten</code> relationship's successors.
     * @param <E> a MtObject class     * @return an object iterator
     *
     * @see #getImparten
     * @see #getImpartenSize
     * @see #setImparten
     * @see #removeImparten
     */
    public final <E extends com.matisse.reflect.MtObject> com.matisse.MtObjectIterator<E> impartenIterator() {
        return this.<E>successorIterator(getImpartenRelationship(getMtDatabase()), academia.Clases.class);
    }

    /**
     * Sets the <code>imparten</code> relationship's successors.
     * @param succs an array of objects
     *
     * @see #getImparten
     * @see #getImpartenSize
     * @see #impartenIterator
     * @see #removeImparten
     */
    public final void setImparten(academia.Clases[] succs) {
        setSuccessors(getImpartenRelationship(getMtDatabase()), succs);
    }

    /**
     * Inserts one object at the beginning of the existing <code>imparten</code> successors list.
     * @param succ the object to add
     *
     * @see #getImparten
     * @see #getImpartenSize
     * @see #impartenIterator
     * @see #setImparten
     * @see #removeImparten
     */
    public final void prependImparten(academia.Clases succ) {
        prependSuccessor(getImpartenRelationship(getMtDatabase()), succ);
    }

    /**
     * Adds one object to the end of the existing <code>imparten</code> successors list.
     * @param succ the object to add
     *
     * @see #getImparten
     * @see #setImparten
     * @see #removeImparten
     * @see #getImpartenSize
     * @see #impartenIterator
     */
    public final void appendImparten(academia.Clases succ) {
        appendSuccessor(getImpartenRelationship(getMtDatabase()), succ);
    }
    /** Adds multiple objects to the end of the existing <code>imparten</code> successors list.
     * @param succs an array of objects to add
     *
     * @see #getImparten
     * @see #setImparten
     * @see #removeImparten
     * @see #getImpartenSize
     * @see #impartenIterator
     */
    public final void appendImparten(academia.Clases[] succs) {
        addSuccessors(getImpartenRelationship(getMtDatabase()), succs);
    }

    /**
     * Removes objects from the <code>imparten</code> successors list.
     * @param succs an array of objects to remove
     *
     * @see #getImparten
     * @see #setImparten
     * @see #getImpartenSize
     * @see #impartenIterator
     */
    public void removeImparten(academia.Clases[] succs) {
        removeSuccessors(getImpartenRelationship(getMtDatabase()), succs);
    }

    /**
     * Removes one object from the <code>imparten</code> successors list.
     * @param succ the object to remove
     *
     * @see #getImparten
     * @see #setImparten
     * @see #getImpartenSize
     * @see #impartenIterator
     */
    public void removeImparten(academia.Clases succ) {
        removeSuccessor(getImpartenRelationship(getMtDatabase()), succ);
    }

    /**
     * Removes all <code>imparten</code> successors.  When the minimum cardinality
     * is 1, a new successor must be set before commit.
     */
    public final void clearImparten() {
        clearSuccessors(getImpartenRelationship(getMtDatabase()));
    }


    // END of Matisse SDL Generated Code

    /*
     * You may add your own code here...
     */

    /**
     * Default constructor provided as an example.
     * You may delete this constructor or modify it to suit your needs. If you
     * modify it, please revise this comment accordingly.
     * @param db a database
     */
    public Profesores(com.matisse.MtDatabase db) {
        super(getClass(db));
    }

    /**
     * Example of <code>toString</code> method.
     * You may delete this method or modify it to suit your needs. If you
     * modify it, please revise this comment accordingly.
     * @return a string
     */
    public java.lang.String toString() {
        return super.toString() + "[Profesores]";
    }
}
