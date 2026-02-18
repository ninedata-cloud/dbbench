package com.ninedata.dbbench.database.plugin;

import java.util.Map;

/**
 * Simple arithmetic expression evaluator supporting variables, +, -, *, /, parentheses.
 * Division by zero returns 0.
 */
public class ExpressionEvaluator {

    private final Map<String, Double> variables;
    private String expr;
    private int pos;

    public ExpressionEvaluator(Map<String, Double> variables) {
        this.variables = variables;
    }

    public static double evaluate(String expression, Map<String, Double> variables) {
        return new ExpressionEvaluator(variables).parse(expression);
    }

    private double parse(String expression) {
        this.expr = expression.replaceAll("\\s+", "");
        this.pos = 0;
        double result = parseAddSub();
        return Double.isNaN(result) || Double.isInfinite(result) ? 0 : result;
    }

    private double parseAddSub() {
        double left = parseMulDiv();
        while (pos < expr.length()) {
            char op = expr.charAt(pos);
            if (op != '+' && op != '-') break;
            pos++;
            double right = parseMulDiv();
            left = op == '+' ? left + right : left - right;
        }
        return left;
    }

    private double parseMulDiv() {
        double left = parseUnary();
        while (pos < expr.length()) {
            char op = expr.charAt(pos);
            if (op != '*' && op != '/') break;
            pos++;
            double right = parseUnary();
            if (op == '/') {
                left = right == 0 ? 0 : left / right;
            } else {
                left = left * right;
            }
        }
        return left;
    }

    private double parseUnary() {
        if (pos < expr.length() && expr.charAt(pos) == '-') {
            pos++;
            return -parsePrimary();
        }
        return parsePrimary();
    }

    private double parsePrimary() {
        if (pos < expr.length() && expr.charAt(pos) == '(') {
            pos++; // skip '('
            double result = parseAddSub();
            if (pos < expr.length() && expr.charAt(pos) == ')') {
                pos++; // skip ')'
            }
            return result;
        }
        // Try number
        int start = pos;
        while (pos < expr.length() && (Character.isDigit(expr.charAt(pos)) || expr.charAt(pos) == '.')) {
            pos++;
        }
        if (pos > start) {
            return Double.parseDouble(expr.substring(start, pos));
        }
        // Variable name (letters, digits, underscores)
        start = pos;
        while (pos < expr.length() && (Character.isLetterOrDigit(expr.charAt(pos)) || expr.charAt(pos) == '_')) {
            pos++;
        }
        if (pos > start) {
            String varName = expr.substring(start, pos);
            return variables.getOrDefault(varName, 0.0);
        }
        return 0;
    }
}
