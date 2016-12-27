grammar ParquetDsl;

query : selectExpr fromExpr whereExpr;

selectExpr : 'select' ('*' | metricExpr);

metricExpr : INT (',' INT)*;

fromExpr : 'from' QUOTE FILEPATH QUOTE;

whereExpr : 'where' conditionExpr;

conditionExpr :
    longEqExpr # LongEq |
    nestedCondition '&&' nestedCondition # And |
    nestedCondition '||' nestedCondition # Or ;

nestedCondition : '(' conditionExpr ')';

longEqExpr : QUOTED_ID '=' INT;

FILEPATH : ('/' | LETTER) (LETTER | DIGIT | '-' | '_' | '/' | '.')+ ;

//QUOTED_FILEPATH : QUOTE FILEPATH QUOTE;

QUOTED_ID : QUOTE ID QUOTE;

ID : LETTER (LETTER | '_' | DIGIT)*;

INT : DIGIT+ ;

fragment DIGIT : '0'..'9';

fragment LETTER : ('a'..'z' | 'A'..'Z') ;

WS : (' ' | '\r' | '\t' | '\n')+ -> skip ;

QUOTE : '\'';