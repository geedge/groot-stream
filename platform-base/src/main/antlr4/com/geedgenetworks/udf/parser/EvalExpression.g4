grammar EvalExpression;

@members {
  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or letter or underscore.
   *
   * For example:
   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is folllowed
   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
   * which is not a digit or letter or underscore.
   */
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }
}

statement
    : valueExpression #assignValue
    | booleanExpression '?' trueValue=valueExpression ':' falseValue=valueExpression  #predicated
    ;

booleanExpression
    : left=valueExpression operator=comparisonOperator right=valueExpression #valueComparison
    | left=booleanExpression operator=predicateOperator right=booleanExpression #logicalComparison
    | NOT booleanExpression #notComparison
    ;

valueExpression
    : constant #constantValue
    | identifier #variable
    | left=valueExpression operator=arithmeticOperator right=valueExpression #arithmeticValue
    ;

constant
    : NULL #nullLiteral
    | number #numericLiteral
    | (TRUE | FALSE) #booleanLiteral
    | STRING #stringLiteral
    ;

identifier
    : IDENTIFIER
    ;

number
    : MINUS? INTEGER_VALUE #integerLiteral
    | MINUS? BIGINT_LITERAL #bigIntLiteral
    | MINUS? SMALLINT_LITERAL #smallIntLiteral
    | MINUS? DOUBLE_LITERAL #doubleLiteral
    | MINUS? FLOAT_LITERAL #floatLiteral
    | MINUS? BIGDECIMAL_LITERAL #bigDecimalLiteral
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

arithmeticOperator
    : PLUS | MINUS | ASTERISK | SLASH | PERCENT
    ;

predicateOperator
    : OR | AND
    ;

NULL: 'NULL';

EQ  : '=' | '==';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=' | '!>';
GT  : '>';
GTE : '>=' | '!<';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';

AND: '&' | '&&';
OR: '|' | '||';
NOT: '!';

TRUE: 'TRUE';
FALSE: 'FALSE';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

FLOAT_LITERAL
    : DIGIT+ EXPONENT? 'F'
    | DECIMAL_DIGITS EXPONENT? 'F' {isValidDecimal()}?
    ;

DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D' {isValidDecimal()}?
    ;

BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD' {isValidDecimal()}?
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;


fragment LETTER
    : [a-zA-Z]
    ;

fragment DIGIT
    : [0-9]
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

UNRECOGNIZED
    : .
    ;