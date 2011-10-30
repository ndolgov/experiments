package net.ndolgov.antlrtest;

/**
 * Embedment Helper object (see http://martinfowler.com/dslCatalog/embedmentHelper.html) called by ANTLR-generated query parser.
 *
 */
interface EmbedmentHelper {
    /**
     * Set storage id
     * @param id storage id
     */
    void onStorage(String id);

    /**
     * Add variable definition
     * @param type variable type
     * @param name variable name
     */
    void onVariable(Type type, String name);
}
