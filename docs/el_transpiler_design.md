# NiFi Expression Language Transpiler Design

## Executive Summary

This document describes the design of a transpiler that converts Apache NiFi Expression Language (EL) to equivalent Python code. The transpiler is a critical component of the nifi2py project, which converts NiFi flows to Python.

**Key Design Decisions:**
- Use Lark parser (Python) instead of ANTLR (to avoid Java dependency)
- Build AST-based transpiler (not regex-based) for proper method chaining
- Handle embedded expressions recursively
- Maintain null-safe behavior matching NiFi's semantics

---

## 1. Grammar Overview

### 1.1 Expression Structure

NiFi EL has the following structure:
```
${subject:function1(arg):function2(arg):...}
```

**Components:**
- `${}` - Expression delimiters
- `subject` - Attribute name or nested expression
- `:` - Function chain operator
- `function(args)` - Method calls with optional arguments
- Embedded expressions allowed in arguments: `${attr:equals(${other})}`

**Special Cases:**
- `$$` - Escaped dollar sign (literal `$`)
- `#{param}` - Parameter reference (distinct from attribute reference)
- String literals: `'value'` or `"value"` with escape sequences
- Numeric literals: `123`, `45.67`, `-1.5E-2`
- Boolean literals: `true`, `false`
- Null literal: `null`

### 1.2 Function Categories

From ANTLR grammar analysis:

| Category | Function Type | Examples |
|----------|--------------|----------|
| **Attribute Selection** | Multi-value selectors | `anyAttribute`, `allAttributes`, `anyMatchingAttribute` |
| **No-Subject Functions** | Standalone generators | `UUID()`, `now()`, `nextInt()`, `hostname()` |
| **String Functions** | 0-arg | `toUpper()`, `toLower()`, `trim()`, `urlEncode()` |
| | 1-arg | `substringBefore(s)`, `substringAfter(s)`, `append(s)`, `prepend(s)` |
| | 2-arg | `substring(start, end)`, `replace(old, new)`, `ifElse(a, b)` |
| | 5-arg | `getDelimitedField(...)` |
| **Boolean Functions** | 0-arg | `isNull()`, `isEmpty()`, `not()` |
| | 1-arg | `equals(s)`, `startsWith(s)`, `endsWith(s)`, `contains(s)` |
| | Multi-arg | `in(val1, val2, ...)` |
| **Numeric Functions** | 0-arg | `length()`, `toNumber()`, `count()` |
| | 1-arg | `indexOf(s)`, `plus(n)`, `multiply(n)`, `mod(n)` |
| | Variable-arg | `math(fn)`, `math(fn, arg)`, `toDate(fmt, tz)` |
| **Date/Time Functions** | Conversions | `toDate(fmt)`, `toInstant(fmt, tz)`, `format(fmt)` |
| | Generators | `now()` |

---

## 2. Lark Grammar Design

### 2.1 Grammar Skeleton

```lark
?start: expression

expression: DOLLAR LBRACE attr_or_function (COLON function_call)* RBRACE

attr_or_function: attribute_ref
                | standalone_function
                | parameter_ref

attribute_ref: ATTRIBUTE_NAME
             | STRING_LITERAL
             | expression

parameter_ref: PARAMETER_START ATTRIBUTE_NAME RBRACE

standalone_function: no_subject_func LPAREN RPAREN
                   | no_subject_func LPAREN arg RPAREN

function_call: string_func
             | boolean_func
             | numeric_func
             | date_func

// 0-arg functions
zero_arg_string: ("toUpper" | "toLower" | "trim" | "urlEncode" | "urlDecode"
                | "base64Encode" | "base64Decode" | "escapeJson" | "escapeXml") LPAREN RPAREN

// 1-arg functions
one_arg_string: ("substringBefore" | "substringAfter" | "substringBeforeLast" | "substringAfterLast"
               | "append" | "prepend" | "startsWith" | "endsWith" | "contains"
               | "replaceNull" | "replaceEmpty") LPAREN arg RPAREN

// 2-arg functions
two_arg_string: ("substring" | "replace" | "replaceFirst" | "replaceAll" | "ifElse"
               | "padLeft" | "padRight") LPAREN arg COMMA arg RPAREN

// Arguments can be nested expressions
arg: NUMBER
   | STRING_LITERAL
   | BOOLEAN
   | NULL
   | expression
   | function_call

// Terminals
DOLLAR: "$"
LBRACE: "{"
RBRACE: "}"
LPAREN: "("
RPAREN: ")"
COLON: ":"
COMMA: ","
PARAMETER_START: "#{"

STRING_LITERAL: /"(?:[^"\\]|\\.)*"/ | /'(?:[^'\\]|\\.)*'/
ATTRIBUTE_NAME: /[a-zA-Z_][a-zA-Z0-9_\-\.]*/
NUMBER: /-?\d+(\.\d+)?([eE][+-]?\d+)?/
BOOLEAN: "true" | "false"
NULL: "null"

%import common.WS
%ignore WS
```

### 2.2 Special Handling Requirements

**Embedded Expressions:**
- Parser must handle recursive nesting: `${x:equals(${y:toUpper()})}`
- Argument evaluation order matters for side effects (though rare in EL)

**Escape Sequences:**
- `$$` → `$` (literal dollar in output)
- String escape sequences: `\n`, `\t`, `\r`, `\\`, `\"`, `\'`

**Multi-attribute Functions:**
- `allAttributes('a', 'b', 'c')` returns multiple values
- Must be reduced with functions like `join()` or `count()`
- Error if non-reducing function used on multi-value subject

---

## 3. Function Mapping: NiFi → Python

### 3.1 Priority 1 (P1) Functions

These functions are essential for basic flow conversion:

#### String Functions

| NiFi EL | Python Equivalent | Notes |
|---------|------------------|-------|
| `${attr}` | `attributes.get('attr', '')` | Default empty string on missing |
| `${attr:toUpper()}` | `attributes.get('attr', '').upper()` | Null-safe |
| `${attr:toLower()}` | `attributes.get('attr', '').lower()` | Null-safe |
| `${attr:trim()}` | `attributes.get('attr', '').strip()` | Removes whitespace |
| `${attr:substring(2, 5)}` | `attributes.get('attr', '')[2:5]` | Python slice notation |
| `${attr:substringBefore('.')}` | `attributes.get('attr', '').split('.', 1)[0] if '.' in attributes.get('attr', '') else attributes.get('attr', '')` | Complex logic |
| `${attr:substringAfter('.')}` | `attributes.get('attr', '').split('.', 1)[1] if '.' in attributes.get('attr', '') else attributes.get('attr', '')` | Returns whole string if not found |
| `${attr:substringBeforeLast('.')}` | `attributes.get('attr', '').rsplit('.', 1)[0] if '.' in attributes.get('attr', '') else attributes.get('attr', '')` | Use rsplit for last occurrence |
| `${attr:substringAfterLast('.')}` | `attributes.get('attr', '').rsplit('.', 1)[1] if '.' in attributes.get('attr', '') else attributes.get('attr', '')` | Use rsplit for last occurrence |
| `${attr:append('_suffix')}` | `attributes.get('attr', '') + '_suffix'` | String concatenation |
| `${attr:prepend('prefix_')}` | `'prefix_' + attributes.get('attr', '')` | String concatenation |
| `${attr:replace('old', 'new')}` | `attributes.get('attr', '').replace('old', 'new')` | Replace all occurrences |
| `${attr:replaceAll('regex', 'new')}` | `re.sub(r'regex', 'new', attributes.get('attr', ''))` | Requires `import re` |
| `${attr:replaceFirst('regex', 'new')}` | `re.sub(r'regex', 'new', attributes.get('attr', ''), count=1)` | Requires `import re` |
| `${attr:length()}` | `len(attributes.get('attr', ''))` | Returns integer |
| `${attr:indexOf('x')}` | `attributes.get('attr', '').find('x')` | Returns -1 if not found (NiFi returns -1) |
| `${attr:lastIndexOf('x')}` | `attributes.get('attr', '').rfind('x')` | Returns -1 if not found |
| `${attr:replaceNull('default')}` | `attributes.get('attr', 'default')` | Provide default value |
| `${attr:replaceEmpty('default')}` | `attributes.get('attr', '') or 'default'` | Replace empty string |

#### Logic/Boolean Functions

| NiFi EL | Python Equivalent | Notes |
|---------|------------------|-------|
| `${attr:isEmpty()}` | `not bool(attributes.get('attr', '').strip())` | True for null, empty, or whitespace |
| `${attr:isNull()}` | `'attr' not in attributes or attributes.get('attr') is None` | Check existence |
| `${attr:notNull()}` | `'attr' in attributes and attributes.get('attr') is not None` | Opposite of isNull |
| `${attr:equals('value')}` | `attributes.get('attr', '') == 'value'` | Case-sensitive comparison |
| `${attr:equalsIgnoreCase('value')}` | `attributes.get('attr', '').lower() == 'value'.lower()` | Case-insensitive |
| `${attr:startsWith('prefix')}` | `attributes.get('attr', '').startswith('prefix')` | Boolean result |
| `${attr:endsWith('.txt')}` | `attributes.get('attr', '').endswith('.txt')` | Boolean result |
| `${attr:contains('sub')}` | `'sub' in attributes.get('attr', '')` | Boolean result |
| `${attr:matches('regex')}` | `bool(re.match(r'regex', attributes.get('attr', '')))` | Full regex match |
| `${attr:find('regex')}` | `bool(re.search(r'regex', attributes.get('attr', '')))` | Partial regex match |
| `${cond:and(${other})}` | `condition and other_condition` | Logical AND |
| `${cond:or(${other})}` | `condition or other_condition` | Logical OR |
| `${cond:not()}` | `not condition` | Logical NOT |
| `${attr:ifElse('true_val', 'false_val')}` | `'true_val' if attributes.get('attr', '') else 'false_val'` | Ternary expression |
| `${attr:in('a', 'b', 'c')}` | `attributes.get('attr', '') in ['a', 'b', 'c']` | Membership test |

#### Numeric Functions

| NiFi EL | Python Equivalent | Notes |
|---------|------------------|-------|
| `${attr:toNumber()}` | `int(attributes.get('attr', '0'))` | Parse to integer |
| `${attr:toDecimal()}` | `float(attributes.get('attr', '0.0'))` | Parse to float |
| `${attr:plus(5)}` | `(int(attributes.get('attr', '0')) if '.' not in attributes.get('attr', '0') else float(attributes.get('attr', '0.0'))) + 5` | Auto-detect int/float |
| `${attr:minus(5)}` | `(int(attributes.get('attr', '0')) if '.' not in attributes.get('attr', '0') else float(attributes.get('attr', '0.0'))) - 5` | Auto-detect int/float |
| `${attr:multiply(3)}` | `(int(attributes.get('attr', '0')) if '.' not in attributes.get('attr', '0') else float(attributes.get('attr', '0.0'))) * 3` | Auto-detect int/float |
| `${attr:divide(2)}` | `(int(attributes.get('attr', '0')) if '.' not in attributes.get('attr', '0') else float(attributes.get('attr', '0.0'))) / 2` | Auto-detect int/float |
| `${attr:mod(10)}` | `(int(attributes.get('attr', '0')) if '.' not in attributes.get('attr', '0') else float(attributes.get('attr', '0.0'))) % 10` | Modulo operation |
| `${attr:gt(5)}` | `(int(attributes.get('attr', '0')) if '.' not in attributes.get('attr', '0') else float(attributes.get('attr', '0.0'))) > 5` | Greater than |
| `${attr:lt(5)}` | `(int(attributes.get('attr', '0')) if '.' not in attributes.get('attr', '0') else float(attributes.get('attr', '0.0'))) < 5` | Less than |
| `${attr:ge(5)}` | `(int(attributes.get('attr', '0')) if '.' not in attributes.get('attr', '0') else float(attributes.get('attr', '0.0'))) >= 5` | Greater than or equal |
| `${attr:le(5)}` | `(int(attributes.get('attr', '0')) if '.' not in attributes.get('attr', '0') else float(attributes.get('attr', '0.0'))) <= 5` | Less than or equal |

#### Date/Time Functions

| NiFi EL | Python Equivalent | Notes |
|---------|------------------|-------|
| `${now()}` | `datetime.now()` | Current timestamp |
| `${now():format('yyyy-MM-dd')}` | `datetime.now().strftime('%Y-%m-%d')` | See date format table below |
| `${attr:toDate('yyyy/MM/dd')}` | `datetime.strptime(attributes.get('attr', ''), '%Y/%m/%d').timestamp() * 1000` | Returns milliseconds |
| `${timestamp:toDate():format('yyyy')}` | `datetime.fromtimestamp(int(attributes.get('timestamp', '0')) / 1000).strftime('%Y')` | From millis to formatted |

#### Special Functions

| NiFi EL | Python Equivalent | Notes |
|---------|------------------|-------|
| `${UUID()}` | `str(uuid.uuid4())` | Generate random UUID |
| `${literal('text')}` | `'text'` | Return literal value |

### 3.2 Helper Functions Required

To make the generated Python code readable, create helper functions:

```python
def _get_attr(attributes: dict, name: str, default: str = '') -> str:
    """Null-safe attribute getter matching NiFi semantics."""
    value = attributes.get(name, default)
    return '' if value is None else str(value)

def _to_number(value: str) -> Union[int, float]:
    """Convert string to int or float based on presence of decimal point."""
    if not value:
        return 0
    if '.' in value or 'e' in value.lower():
        return float(value)
    return int(value)

def _substring_before(text: str, delimiter: str) -> str:
    """Return substring before first occurrence of delimiter."""
    if not delimiter:
        return text
    idx = text.find(delimiter)
    return text[:idx] if idx >= 0 else text

def _substring_after(text: str, delimiter: str) -> str:
    """Return substring after first occurrence of delimiter."""
    if not delimiter:
        return text
    idx = text.find(delimiter)
    return text[idx + len(delimiter):] if idx >= 0 else text

def _is_empty(value: str) -> bool:
    """Check if value is null, empty, or whitespace."""
    return not bool(value and value.strip())
```

---

## 4. Date Format Conversion Table

NiFi uses Java SimpleDateFormat; Python uses strftime. Key conversions:

| Java Pattern | Python Pattern | Description | Example |
|--------------|---------------|-------------|---------|
| `yyyy` | `%Y` | 4-digit year | 2024 |
| `yy` | `%y` | 2-digit year | 24 |
| `MMMM` | `%B` | Full month name | January |
| `MMM` | `%b` | Abbreviated month | Jan |
| `MM` | `%m` | 2-digit month | 01 |
| `dd` | `%d` | Day of month | 05 |
| `HH` | `%H` | Hour (24-hour) | 14 |
| `hh` | `%I` | Hour (12-hour) | 02 |
| `mm` | `%M` | Minute | 30 |
| `ss` | `%S` | Second | 45 |
| `SSS` | `%f` | Microseconds (note: different precision!) | 123000 |
| `a` | `%p` | AM/PM marker | PM |
| `EEEE` | `%A` | Day of week (full) | Monday |
| `EEE` | `%a` | Day of week (abbr) | Mon |
| `D` | `%j` | Day of year | 123 |
| `z` | `%Z` | Timezone name | EST |
| `Z` | `%z` | Timezone offset | +0500 |

**Important Differences:**
- Java `SSS` is milliseconds (000-999), Python `%f` is microseconds (000000-999999)
  - Convert: multiply/divide by 1000
- Java allows literals in quotes: `'T'` → Python just uses T
- Java timezone handling is more complex; may need `pytz` library

**Implementation:**
```python
def _convert_date_format(java_format: str) -> str:
    """Convert Java SimpleDateFormat to Python strftime format."""
    conversions = {
        'yyyy': '%Y',
        'yy': '%y',
        'MMMM': '%B',
        'MMM': '%b',
        'MM': '%m',
        'dd': '%d',
        'HH': '%H',
        'hh': '%I',
        'mm': '%M',
        'ss': '%S',
        'SSS': '%f',  # Note: need to handle ms vs µs
        'a': '%p',
        'EEEE': '%A',
        'EEE': '%a',
        'D': '%j',
        'z': '%Z',
        'Z': '%z',
    }

    result = java_format
    # Replace in order of length (longest first to avoid partial matches)
    for java_pat, python_pat in sorted(conversions.items(), key=lambda x: -len(x[0])):
        result = result.replace(java_pat, python_pat)

    # Handle quoted literals: 'T' → T
    result = re.sub(r"'([^']*)'", r'\1', result)

    return result
```

---

## 5. Edge Cases and Special Handling

### 5.1 Null Safety

NiFi EL is null-safe; missing attributes return empty string, not error:

```python
# NiFi: ${missing_attr:toUpper()} → ""
# Python: attributes.get('missing_attr', '').upper() → ""
```

All transpiled code must maintain this behavior.

### 5.2 Embedded Expressions

```python
# NiFi: ${filename:replaceAll( ${pattern}, ${replacement} )}
# Must evaluate inner expressions first:
pattern_value = attributes.get('pattern', '')
replacement_value = attributes.get('replacement', '')
result = re.sub(pattern_value, replacement_value, attributes.get('filename', ''))
```

**Strategy:** Build AST, evaluate recursively from leaves to root.

### 5.3 Type Coercion

NiFi auto-converts types; Python must do same:

```python
# NiFi: ${count:plus(1)} where count="5" → 6 (numeric)
# Python must parse string to int:
int(attributes.get('count', '0')) + 1
```

### 5.4 Method Chaining

```python
# NiFi: ${filename:substringBefore('.'):toUpper():append('.TXT')}
# Must chain left-to-right:
temp1 = _substring_before(attributes.get('filename', ''), '.')
temp2 = temp1.upper()
result = temp2 + '.TXT'

# Or as one expression:
(_substring_before(attributes.get('filename', ''), '.').upper() + '.TXT')
```

### 5.5 Multi-Value Attributes

```python
# NiFi: ${allAttributes('a', 'b'):join(',')}
# Returns: "value_a,value_b"
# Must collect multiple values then reduce

values = [attributes.get(k, '') for k in ['a', 'b'] if k in attributes]
result = ','.join(values)
```

### 5.6 Escape Sequences

```
# Input: "prefix_$${attr}_suffix"
# NiFi evaluates to: "prefix_${attr}_suffix" (literal ${})
# Must detect $$ and skip evaluation
```

---

## 6. Transpiler Architecture

### 6.1 Class Structure

```python
from lark import Lark, Transformer
from typing import Any, Dict, List

class ELTranspiler:
    """Transpile NiFi Expression Language to Python."""

    def __init__(self):
        self.parser = Lark(GRAMMAR, start='start', parser='lalr')

    def transpile(self, el_expression: str) -> str:
        """Convert EL expression to Python code."""
        # Parse to AST
        tree = self.parser.parse(el_expression)

        # Transform AST to Python code
        transformer = ELToPythonTransformer()
        python_code = transformer.transform(tree)

        return python_code

    def transpile_embedded(self, text: str) -> str:
        """Handle text with embedded EL expressions."""
        # Find all ${...} ranges
        # Transpile each
        # Reconstruct string
        ...

class ELToPythonTransformer(Transformer):
    """Lark transformer to convert AST to Python."""

    def expression(self, items):
        subject, *functions = items
        code = subject
        for func in functions:
            code = self._apply_function(code, func)
        return code

    def attribute_ref(self, items):
        attr_name = items[0]
        return f"attributes.get('{attr_name}', '')"

    def function_call(self, items):
        func_name, *args = items
        return self._generate_function_call(func_name, args)

    def _generate_function_call(self, name: str, args: List[str]) -> str:
        """Map NiFi function to Python equivalent."""
        # Use mapping table from Section 3
        ...
```

### 6.2 Processing Pipeline

```
Input EL String
    ↓
[1. Escape Handling]  # Detect $$ → $
    ↓
[2. Lexical Analysis] # Tokenize
    ↓
[3. Parsing]          # Build AST
    ↓
[4. AST Transformation] # NiFi functions → Python
    ↓
[5. Code Generation]  # Emit Python string
    ↓
Output Python Code
```

---

## 7. Test Strategy

### 7.1 Unit Tests

Test each function mapping individually:

```python
def test_substring_before():
    transpiler = ELTranspiler()

    # Basic case
    result = transpiler.transpile("${filename:substringBefore('.')}")
    assert result == "_substring_before(attributes.get('filename', ''), '.')"

    # Edge case: delimiter not found
    attrs = {'filename': 'noextension'}
    exec_result = eval(result, {'attributes': attrs, '_substring_before': _substring_before})
    assert exec_result == 'noextension'

    # Edge case: empty string
    attrs = {'filename': ''}
    exec_result = eval(result, {'attributes': attrs, '_substring_before': _substring_before})
    assert exec_result == ''
```

### 7.2 Integration Tests

Test complex expressions from actual NiFi flows:

```python
def test_complex_date_formatting():
    """Test from real NiFi template."""
    el = "${now():format('yyyy-MM-dd\\'T\\'HH:mm:ss.SSS\\'Z\\'')}"

    python_code = transpiler.transpile(el)

    # Execute and verify format
    result = eval(python_code, {'datetime': datetime})
    assert re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z', result)
```

### 7.3 Parity Tests

Use NiFi test cases from `TestQuery.java`:

```python
# Extract test case from NiFi source:
# verifyEquals("${attr:trim():substring(2, 5)}", attributes, " Va")

def test_parity_trim_substring():
    """Verify parity with NiFi TestQuery.java:772-775."""
    attrs = {"attr": "   My Value   "}
    el = "${attr:trim():substring(2, 5)}"

    python_code = transpiler.transpile(el)
    result = eval(python_code, {'attributes': attrs})

    assert result == " Va"  # Expected from NiFi test
```

---

## 8. Implementation Phases

### Phase 1: Core Parser (Week 1)
- [ ] Implement Lark grammar
- [ ] Parse simple expressions: `${attr}`, `${attr:toUpper()}`
- [ ] Handle string literals and basic functions
- [ ] Unit tests for parser

### Phase 2: Function Mappings (Week 1-2)
- [ ] Implement P1 string functions
- [ ] Implement P1 boolean functions
- [ ] Implement basic numeric functions
- [ ] Helper function library

### Phase 3: Advanced Features (Week 2)
- [ ] Embedded expression support
- [ ] Date/time functions + format conversion
- [ ] Multi-value attribute handling
- [ ] Escape sequence handling

### Phase 4: Testing & Validation (Week 3)
- [ ] Extract 50+ test cases from NiFi TestQuery.java
- [ ] Property-based testing with Hypothesis
- [ ] Parity validation against NiFi behavior
- [ ] Documentation and examples

---

## 9. Known Limitations

### Current Scope Exclusions

1. **Advanced Functions (P2/P3):**
   - JSON path operations (`jsonPath`, `jsonPathSet`, etc.)
   - Encoding functions (`base64Encode`, `urlEncode`)
   - Hash functions
   - Math library functions
   - Regex groups in replaceAll

2. **Multi-Value Reduce Functions:**
   - Initial version will error on unreduced multi-value subjects
   - Future: implement `join()`, `count()`, etc.

3. **State Functions:**
   - `getStateValue()` requires NiFi state context
   - May need to be stubbed or require external state dict

4. **Parameter References:**
   - `#{param}` references will be supported but require separate parameter dict

### Compatibility Notes

- **Numeric Precision:** Python float vs Java double may have minor differences
- **Date Milliseconds:** Python datetime uses microseconds; conversion needed
- **Regex Flavor:** Python `re` vs Java Pattern have minor syntax differences
- **Locale:** NiFi uses US locale for date formatting; Python should too

---

## 10. Example Transpilations

### Simple Attribute Access
```
NiFi:   ${filename}
Python: attributes.get('filename', '')
```

### Method Chaining
```
NiFi:   ${filename:substringBefore('.'):toUpper()}
Python: _substring_before(attributes.get('filename', ''), '.').upper()
```

### Embedded Expression
```
NiFi:   ${filename:equals(${expected})}
Python: attributes.get('filename', '') == attributes.get('expected', '')
```

### Date Formatting
```
NiFi:   ${now():format('yyyy-MM-dd HH:mm:ss')}
Python: datetime.now().strftime('%Y-%m-%d %H:%M:%S')
```

### Complex Logic
```
NiFi:   ${filename:endsWith('.json'):ifElse('valid', 'invalid')}
Python: 'valid' if attributes.get('filename', '').endswith('.json') else 'invalid'
```

### Literal with Embedded Expression
```
NiFi:   "prefix_${attr}_suffix"
Python: f"prefix_{attributes.get('attr', '')}_suffix"
```

---

## 11. References

- NiFi EL Grammar: `AttributeExpressionParser.g`, `AttributeExpressionLexer.g`
- NiFi Test Suite: `TestQuery.java` (3000+ lines of test cases)
- Function Implementations: `/nifi-commons/nifi-expression-language/src/main/java/.../functions/`
- Lark Parser Documentation: https://lark-parser.readthedocs.io/
- Python strftime: https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
