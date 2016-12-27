grammar QueryDsl;

import Common;

@parser::header {
import net.ndolgov.querydsl.ast.expression.PredicateExpr;
}

@parser::members {
    private AstBuilder builder;

    public <T extends AstBuilder> T parseWithAstBuilder(T builder) throws RecognitionException {
        this.builder = builder;
        query();
        return builder;
    }
}

query : selectExpr fromExpr whereExpr;

selectExpr : 'select' ('*' | metricExpr);

metricExpr : INT {builder.onMetricId($INT.text);} (',' INT)* {builder.onMetricId($INT.text);} ;

fromExpr : 'from' QUOTE FILEPATH QUOTE {builder.onFromFilePath($FILEPATH.text);} ;

whereExpr : 'where' pred = conditionExpr {builder.onPredicate($pred.cond);} ;

conditionExpr returns [PredicateExpr cond] :
    pred = longEqExpr { $cond = $pred.cond; } |
    left = nestedCondition '&&' right = nestedCondition {$cond = builder.onAnd($left.cond, $right.cond);} |
    left = nestedCondition '||' right = nestedCondition {$cond = builder.onOr($left.cond, $right.cond);} ;

nestedCondition returns [PredicateExpr cond] : '(' pred = conditionExpr ')' {$cond = $pred.cond;} ;

longEqExpr returns [PredicateExpr cond] : QUOTED_ID '=' INT {$cond = builder.onAttrEqLong($QUOTED_ID.text, $INT.text);} ;
