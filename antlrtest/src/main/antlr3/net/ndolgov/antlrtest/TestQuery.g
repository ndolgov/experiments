grammar TestQuery;

tokens {
	SELECT = 'SELECT' ;
	FROM = 'FROM' ;
	LONG_TYPE = 'LONG';
	DOUBLE_TYPE = 'DOUBLE';
}

@header {
package net.ndolgov.antlrtest;

import org.antlr.runtime.*;
import java.io.IOException;
}

@members {
    private EmbedmentHelper helper;

    public <EH extends EmbedmentHelper> EH parseWithHelper(EH helper) throws RecognitionException {
        this.helper = helper;
        query();
        return helper;
    }

    public void emitErrorMessage(String msg) {
        throw new IllegalArgumentException("Query parser error: " + msg);
    }
}

@lexer::header {
package net.ndolgov.antlrtest;
}

query	: SELECT variable (',' variable )*
	  FROM ID {helper.onStorage($ID.text);};
		
variable : type=varType id=ID {helper.onVariable(type, $id.text);};
					
varType returns [Type type]
	: LONG_TYPE {$type = Type.LONG;}
	| DOUBLE_TYPE {$type = Type.DOUBLE;};
	
ID :  ('a'..'z'|'A'..'Z'|'_'|'$')+;

WS  :   ( ' '
        | '\t'
        | '\r'
        | '\n'
        ) {$channel=HIDDEN;};
	
	
	
	