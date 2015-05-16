avpath
======

XPath likeness for Avro

.. image:: https://travis-ci.org/wandoulabs/avpath.png
   :target: https://travis-ci.org/wandoulabs/avpath
   :alt: avpath build status

avpath User Manual
------------------

Preface
-------

avpath is the likeness of xpath/jspath to select, update, insert, delete
data on Avro form. Which could be used by Java/Scala as API library，or
used as data service for Avro records. The expression is similar to
`jspath <https://github.com/dfilatov/jspath>`__, but we also added APIs
to support for **Update**, **Delete**, **Clear**, **Insert** and
**InsertAll**. This manual is based on the User Manual of jspath.

Comparing to jspath that is applied on Json data, avpath is applied on
Avro data that has Map data type, thus leading to an extra experssion to
query map by key:

.. code:: scala

    avpath.select(record, ".mapfield(\"thekey\")")

Usage
-----

Select:
~~~~~~~

.. code:: scala

    import chana.avro.avpath.Evaluator
    import chana.avro.avpath.Parser

    val schema = new Schema.parse("account.avsc")
    val account = new GenericData.Record(schema)
    ...

    var x = avpath.select(record, ".chargeRecords[0].time")

Update:
~~~~~~~

.. code:: scala

    avpath.update(record, ".chargeRecords[0].time", 100000)

Delete (Only applicable on elements of Array/Map):
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: scala

    avpath.delete(record, ".chargeRecords[*]{.time < 1000}")

Clear (Only applicable on Array/Map):
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: scala

    avpath.clear(record, ".chargeRecords")

Insert/InsertAll (Only applicable on Array/Map):
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: scala

    avpath.insert(record, ".chargeRecords", chargeRecord)

    avpath.insertAll(record, ".chargeRecords", List(chargeRecord1, chargeRecord2))

where:

+--------+----------------+-----------------+
|        | Type           | Description     |
+========+================+=================+
| path   | string         | path expression |
+--------+----------------+-----------------+
| avro   | any valid avro | input avro data |
+--------+----------------+-----------------+
| schema | schema         |                 |
+--------+----------------+-----------------+

Quick example
~~~~~~~~~~~~~

the first param is pseudo avro record in json format.

.. code:: scala

    avpath.select(
        {
            "automobiles" : [
                { "maker" : "Nissan", "model" : "Teana", "year" : 2011 },
                { "maker" : "Honda", "model" : "Jazz", "year" : 2010 },
                { "maker" : "Honda", "model" : "Civic", "year" : 2007 },
                { "maker" : "Toyota", "model" : "Yaris", "year" : 2008 },
                { "maker" :* "Honda", "model" : "Accord", "year" : 2011 }
            ],
            "motorcycles" : [{ "maker" : "Honda", "model" : "ST1300", "year" : 2012 }]
        },
        ".automobiles{.maker === \"Honda\" && .year > 2009}.model"
        )

Result will be:

::

    ['Jazz', 'Accord']

Documentation
-------------

avpath expression consists of two type of top-level expressions:
location path (required) and predicates (optional).

Location path
~~~~~~~~~~~~~

To select items in avpath, you use a location path. A location path
consists of one or more location steps. Every location step starts with
dot (.) or two dots (..) depending on the item you're trying to select:

-  ``.property`` — locates property immediately descended from context
   items

-  ``..property`` **[TODO]** — locates property deeply descended from
   context items

-  ``.`` — locates context items itself

You can use the wildcard symbol (\*) instead of exact name of property:

-  ``.*`` — locates all properties immediately descended from the
   context items

-  ``..*`` **[TODO]** — locates all properties deeply descended from the
   context items

Also avpath allows to join several properties:

-  ``(.property1 | .property2 | .propertyN)`` — locates property1,
   property2, propertyN immediately descended from context items

-  or even ``(.property1 | .property2.property2_1.property2_1_1)`` —
   locates .property1, .property2.property2\_1.property2\_1\_1 items

Your location path can be absolute or relative. If location path starts
with the root (^) you are using an absolute location path — your
location path begins from the root items.

Consider the following Avro data (**expressed in JSON for
convenience**):

.. code:: json

    var doc = 
    """
    {
        "books" : [
            {
                "id"     : 1,
                "title"  : "Clean Code",
                "author" : { "name" : "Robert C. Martin" },
                "price"  : 17.96
            },
            {
                "id"     : 2,
                "title"  : "Maintainable JavaScript",
                "author" : { "name" : "Nicholas C. Zakas" },
                "price"  : 10
            },
            {
                "id"     : 3,
                "title"  : "Agile Software Development",
                "author" : { "name" : "Robert C. Martin" },
                "price"  : 20
            },
            {
                "id"     : 4,
                "title"  : "JavaScript: The Good Parts",
                "author" : { "name" : "Douglas Crockford" },
                "price"  : 15.67
            }
        ]
    };

    """

Examples
^^^^^^^^

.. code:: scala

    // find all books authors
    avpath.select(doc, ".books.author")
    // [{ name : 'Robert C. Martin' }, { name : 'Nicholas C. Zakas' }, { name : 'Robert C. Martin' }, { name : 'Douglas Crockford' }]

    // find all books author names
    avpath.select(doc, ".books.author.name")
    // ['Robert C. Martin', 'Nicholas C. Zakas', 'Robert C. Martin', 'Douglas Crockford' ] 

    // find all names in books*
    avpath.select(doc, ".books..name")
    // ['Robert C. Martin', 'Nicholas C. Zakas', 'Robert C. Martin', 'Douglas Crockford' ] 

Predicates
~~~~~~~~~~

avpath predicates allow you to write very specific rules about items
you'd like to select when constructing your expressions. Predicates are
filters that restrict the items selected by location path. There're two
possible types of predicates: object and positional.

Object predicates
~~~~~~~~~~~~~~~~~

Object predicates can be used in a path expression to filter a subset of
items according to a boolean expressions working on a properties of each
item. Object predicates are embedded in braces.

Basic expressions in object predicates:

-  numeric literals (e.g. 1.23)

-  string literals (e.g. "John Gold")

-  boolean literals (true/false)

-  subpathes (e.g. .nestedProp.deeplyNestedProp)

avpath allows to use in predicate expressions following types of
operators:

-  comparison operators

-  string comparison operators

-  logical operators

-  arithmetic operators

**Comparison operators**
^^^^^^^^^^^^^^^^^^^^^^^^
+---------+--------------------------------------------------------------------------------+------------------------+
| ``==``  | Returns is true if both operands are equal                                     | ``.books{.id == "1"}`` |
+---------+--------------------------------------------------------------------------------+------------------------+
| ``===`` | Returns true if both operands are strictly equal with no type conversion       | ``.books{.id === 1}``  |
+---------+--------------------------------------------------------------------------------+------------------------+
| ``!=``  | Returns true if the operands are not equal                                     | ``.books{.id != "1"}`` |
+---------+--------------------------------------------------------------------------------+------------------------+
| ``!==`` | Returns true if the operands are not equal and/or not of the same type         | ``.books{.id !== 1}``  |
+---------+--------------------------------------------------------------------------------+------------------------+
| ``>``   | Returns true if the left operand is greater than the right operand             | ``.books{.id > 1}``    |
+---------+--------------------------------------------------------------------------------+------------------------+
| ``>=``  | Returns true if the left operand is greater than or equal to the right operand | ``.books{.id >= 1}``   |
+---------+--------------------------------------------------------------------------------+------------------------+
| ``<``   | Returns true if the left operand is less than the right operand                | ``.books{.id < 1}``    |
+---------+--------------------------------------------------------------------------------+------------------------+
| ``<=``  | Returns true if the left operand is less than or equal to the right operand    | ``.books{.id <= 1}``   |
+---------+--------------------------------------------------------------------------------+------------------------+

Comparison rules:

-  if both operands to be compared are arrays, then the comparison will
   be true if there is an element in the first array and an element in
   the second array such that the result of performing the comparison of
   two elements is true

-  if one operand is array and another is not, then the comparison will
   be true if there is element in array such that the result of
   performing the comparison of element and another operand is true

-  primitives to be compared as usual javascript primitives

If both operands are strings, there're also available additional
comparison operators:

**String comparison operators**
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+---------+-----------------------------------------------------------------------+--------------------------------------+
| ``==``  | Like an usual ``==`` but case insensitive                             | ``.books{.title == "clean code"}``   |
+---------+-----------------------------------------------------------------------+--------------------------------------+
| ``^==`` | Returns true if left operand value beginning with right operand value | ``.books{.title ^== "Javascript"}``  |
+---------+-----------------------------------------------------------------------+--------------------------------------+
| ``^=``  | Like the ``^==`` but case insensitive                                 | ``.books{.title ^= "javascript"}``   |
+---------+-----------------------------------------------------------------------+--------------------------------------+
| ``$==`` | Returns true if left operand value ending with right operand value    | ``.books{.title $== "Javascript"}``  |
+---------+-----------------------------------------------------------------------+--------------------------------------+
| ``$=``  | Like the ``$==`` but case insensitive                                 | ``.books{.title $= "javascript"}``   |
+---------+-----------------------------------------------------------------------+--------------------------------------+
| ``*==`` | Returns true if left operand value contains right operand value       | ``.books{.title \*== "Javascript"}`` |
+---------+-----------------------------------------------------------------------+--------------------------------------+
| ``*=``  | Like the ``\*==`` but case insensitive                                | ``.books{.title \*= "javascript"}``  |
+---------+-----------------------------------------------------------------------+--------------------------------------+

**Logical operators**
^^^^^^^^^^^^^^^^^^^^^

+---------+----------------------------------------+-----------------------------------------------------------------------------+
| ``&&``  | Returns true if both operands are true | ``.books{.price > 19 && .author.name === "Robert C. Martin"}``              |
+---------+----------------------------------------+-----------------------------------------------------------------------------+
| ``||``  | Returns true if either operand is true | ``.books{.title === "Maintainable JavaScript" || .title === "Clean Code"}`` |
+---------+----------------------------------------+-----------------------------------------------------------------------------+
| ``!``   | Returns true if operand is false       | ``.books{!.title}``                                                         |
+---------+----------------------------------------+-----------------------------------------------------------------------------+

Logical operators convert their operands to boolean values using next
rules:

-  if operand is array (as you remember result of applying subpath is
   also array):

   -  if length of array greater than zero, result will be true

   -  else result will be false

-  Casting with double NOT (!!) javascript operator to be used in any
   other cases.

**Arithmetic operators**
^^^^^^^^^^^^^^^^^^^^^^^^
+-------+----------------+
| ``+`` | addition       |
+-------+----------------+
| ``-`` | subtraction    |
+-------+----------------+
| ``*`` | multiplication |
+-------+----------------+
| ``/`` | division       |
+-------+----------------+
| ``%`` | modulus        |
+-------+----------------+

**Operator precedence**
^^^^^^^^^^^^^^^^^^^^^^^
+---------+----------------------------------------+
| 1 (top) | ``! -unary``                           |
+---------+----------------------------------------+
| 2       | ``* / %``                              |
+---------+----------------------------------------+
| 3       | ``+ -binary``                          |
+---------+----------------------------------------+
| 4       | ``>=``                                 |
+---------+----------------------------------------+
| 5       | ``== === != !== ^= ^== $== $= *= *==`` |
+---------+----------------------------------------+
| 6       | ``&&``                                 |
+---------+----------------------------------------+
| 7       | ``||``                                 |
+---------+----------------------------------------+

Parentheses are used to explicitly denote precedence by grouping parts
of an expression that should be evaluated first.

Examples
^^^^^^^^

.. code:: scala

    // find all book titles whose author is Robert C. Martin
    avpath.select(doc, ".books{.author.name === \"Robert C. Martin\"}.title", schema)
    // ['Clean Code', 'Agile Software Development']

    // find all book titles with price less than 17
    avpath.select(doc, ".books{.price < 17}.title", schema)
    // ['Maintainable JavaScript', 'JavaScript: The Good Parts']

Array positional predicates
~~~~~~~~~~~~~~~~~~~~~

Positional predicates allow you to filter items by their context
position. Positional predicates are always embedded in square brackets.

There are four available forms:

-  ``[index]`` — returns index-positioned item in context (first item
   is at index 0), e.g. [3] returns fourth item in context

-  ``[index:]`` — returns items whose index in context is greater or
   equal to index, e.g. [2:] returns items whose index in context is
   greater or equal to 2

-  ``[:index]`` — returns items whose index in context is smaller than
   index, e.g. [:5] returns first five items in context

-  ``[indexFrom:indexTo]`` — returns items whose index in context is
   greater or equal to indexFrom and smaller than indexTo, e.g. [2:5]
   returns three items with indices 2, 3 and 4

Also you can use negative position numbers:

-  ``[-1]`` — returns last item in context

-  ``[-3:]`` — returns last three items in context

Examples
^^^^^^^^

.. code:: Scala

    // find first book title
    avpath.select(doc, ".books[0].title")
    // ['Clean Code']

    // find first title of books
    avpath.select(doc, ".books.title[0]")
    // 'Clean Code'

    // find last book title
    avpath.select(doc, ".books[-1].title")
    // ['JavaScript: The Good Parts']

    // find two first book titles
    avpath.select(doc, ".books[:2].title")
    // ['Clean Code', 'Maintainable JavaScript']

    // find two last book titles
    avpath.select(doc, ".books[-2:].title")
    // ['Agile Software Development', 'JavaScript: The Good Parts']

    // find two book titles from second position
    avpath.select(doc, ".books[1:3].title")
    // ['Maintainable JavaScript', 'Agile Software Development']

Map key predicates
~~~~~~~~~~~~~~~~~~~~~

Key predicates allow you to filter items by their key via regex.
Regex predicates are always embedded in ().

-  ``("key" | ~"key regex")`` — returns items in context (which matches
   regex)

Multiple predicates
~~~~~~~~~~~~~~~~~~~

You can use more than one predicate. The result will contain only items
that match all the predicates.

**Examples**
^^^^^^^^^^^^

.. code:: scala

    // find first book name whose price less than 15 and greater than 5
    avpath.select(doc, ".books{.price < 15}{.price > 5}[0].title")
    // ['Maintainable JavaScript']

Substitutions (TODO)
~~~~~~~~~~~~~~~~~~~~

Substitutions allow you to use runtime-evaluated values in predicates.

Examples
^^^^^^^^

.. code:: scala

    var path = ".books{.author.name === $author}.title"

    // find book name whose author Nicholas C. Zakas
    avpath.select(doc, path, """{ author : 'Nicholas C. Zakas' }""")
    // ['Maintainable JavaScript'] 

    // find books name whose authors Robert C. Martin or Douglas Crockford
    avpath.select(doc, path, { author : """['Robert C. Martin', 'Douglas Crockford']""" })
    // ['Clean Code', 'Agile Software Development', 'JavaScript: The Good Parts']

Result
~~~~~~

Result of applying AvPath is always a List (empty, if found nothing),
excluding case when the last predicate in top-level expression is a
positional predicate with the exact index (e.g. [0], [5], [-1]). In this
case, result is an Option item at the specified index (None if item
hasn't found).
