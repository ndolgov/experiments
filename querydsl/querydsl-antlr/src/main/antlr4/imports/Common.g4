lexer grammar Common;

FILEPATH : ('/' | LETTER) (LETTER | DIGIT | '-' | '_' | '/' | '.')+ ;

//QUOTED_FILEPATH : QUOTE FILEPATH QUOTE;

QUOTED_ID : QUOTE ID QUOTE;

ID : LETTER (LETTER | '_' | DIGIT)*;

INT : DIGIT+ ;

fragment DIGIT : '0'..'9';

fragment LETTER : ('a'..'z' | 'A'..'Z') ;

WS : (' ' | '\r' | '\t' | '\n')+ -> skip ;

QUOTE : '\'';