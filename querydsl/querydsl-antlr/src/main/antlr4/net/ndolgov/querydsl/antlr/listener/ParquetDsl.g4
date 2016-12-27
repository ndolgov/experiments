grammar ParquetDsl;

import Common;

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