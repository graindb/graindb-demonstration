/* A Bison parser, made by GNU Bison 3.0.4.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

#ifndef YY_BASE_YY_THIRD_PARTY_LIBPG_QUERY_GRAMMAR_GRAMMAR_OUT_HPP_INCLUDED
# define YY_BASE_YY_THIRD_PARTY_LIBPG_QUERY_GRAMMAR_GRAMMAR_OUT_HPP_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int base_yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    IDENT = 258,
    FCONST = 259,
    SCONST = 260,
    BCONST = 261,
    XCONST = 262,
    Op = 263,
    ICONST = 264,
    PARAM = 265,
    TYPECAST = 266,
    DOT_DOT = 267,
    COLON_EQUALS = 268,
    EQUALS_GREATER = 269,
    LAMBDA_ARROW = 270,
    LEFT_ARROW = 271,
    LESS_EQUALS = 272,
    GREATER_EQUALS = 273,
    NOT_EQUALS = 274,
    ABORT_P = 275,
    ABSOLUTE_P = 276,
    ACCESS = 277,
    ACTION = 278,
    ADD_P = 279,
    ADMIN = 280,
    AFTER = 281,
    AGGREGATE = 282,
    ALL = 283,
    ALSO = 284,
    ALTER = 285,
    ALWAYS = 286,
    ANALYSE = 287,
    ANALYZE = 288,
    AND = 289,
    ANY = 290,
    ARRAY = 291,
    AS = 292,
    ASC_P = 293,
    ASSERTION = 294,
    ASSIGNMENT = 295,
    ASYMMETRIC = 296,
    AT = 297,
    ATTACH = 298,
    ATTRIBUTE = 299,
    AUTHORIZATION = 300,
    BACKWARD = 301,
    BEFORE = 302,
    BEGIN_P = 303,
    BETWEEN = 304,
    BIGINT = 305,
    BINARY = 306,
    BIT = 307,
    BOOLEAN_P = 308,
    BOTH = 309,
    BY = 310,
    CACHE = 311,
    CALL_P = 312,
    CALLED = 313,
    CASCADE = 314,
    CASCADED = 315,
    CASE = 316,
    CAST = 317,
    CATALOG_P = 318,
    CHAIN = 319,
    CHAR_P = 320,
    CHARACTER = 321,
    CHARACTERISTICS = 322,
    CHECK_P = 323,
    CHECKPOINT = 324,
    CLASS = 325,
    CLOSE = 326,
    CLUSTER = 327,
    COALESCE = 328,
    COLLATE = 329,
    COLLATION = 330,
    COLUMN = 331,
    COLUMNS = 332,
    COMMENT = 333,
    COMMENTS = 334,
    COMMIT = 335,
    COMMITTED = 336,
    CONCURRENTLY = 337,
    CONFIGURATION = 338,
    CONFLICT = 339,
    CONNECTION = 340,
    CONSTRAINT = 341,
    CONSTRAINTS = 342,
    CONTENT_P = 343,
    CONTINUE_P = 344,
    CONVERSION_P = 345,
    COPY = 346,
    COST = 347,
    CREATE_P = 348,
    CROSS = 349,
    CSV = 350,
    CUBE = 351,
    CURRENT_P = 352,
    CURRENT_CATALOG = 353,
    CURRENT_DATE = 354,
    CURRENT_ROLE = 355,
    CURRENT_SCHEMA = 356,
    CURRENT_TIME = 357,
    CURRENT_TIMESTAMP = 358,
    CURRENT_USER = 359,
    CURSOR = 360,
    CYCLE = 361,
    DATA_P = 362,
    DATABASE = 363,
    DAY_P = 364,
    DAYS_P = 365,
    DEALLOCATE = 366,
    DEC = 367,
    DECIMAL_P = 368,
    DECLARE = 369,
    DEFAULT = 370,
    DEFAULTS = 371,
    DEFERRABLE = 372,
    DEFERRED = 373,
    DEFINER = 374,
    DELETE_P = 375,
    DELIMITER = 376,
    DELIMITERS = 377,
    DEPENDS = 378,
    DESC_P = 379,
    DESCRIBE = 380,
    DETACH = 381,
    DICTIONARY = 382,
    DISABLE_P = 383,
    DISCARD = 384,
    DISTINCT = 385,
    DO = 386,
    DOCUMENT_P = 387,
    DOMAIN_P = 388,
    DOUBLE_P = 389,
    DROP = 390,
    EACH = 391,
    EDGE = 392,
    ELSE = 393,
    ENABLE_P = 394,
    ENCODING = 395,
    ENCRYPTED = 396,
    END_P = 397,
    ENUM_P = 398,
    ESCAPE = 399,
    EVENT = 400,
    EXCEPT = 401,
    EXCLUDE = 402,
    EXCLUDING = 403,
    EXCLUSIVE = 404,
    EXECUTE = 405,
    EXISTS = 406,
    EXPLAIN = 407,
    EXPORT_P = 408,
    EXTENSION = 409,
    EXTERNAL = 410,
    EXTRACT = 411,
    FALSE_P = 412,
    FAMILY = 413,
    FETCH = 414,
    FILTER = 415,
    FIRST_P = 416,
    FLOAT_P = 417,
    FOLLOWING = 418,
    FOR = 419,
    FORCE = 420,
    FOREIGN = 421,
    FORWARD = 422,
    FREEZE = 423,
    FROM = 424,
    FULL = 425,
    FUNCTION = 426,
    FUNCTIONS = 427,
    GENERATED = 428,
    GLOB = 429,
    GLOBAL = 430,
    GRANT = 431,
    GRANTED = 432,
    GROUP_P = 433,
    GROUPING = 434,
    HANDLER = 435,
    HAVING = 436,
    HEADER_P = 437,
    HOLD = 438,
    HOUR_P = 439,
    HOURS_P = 440,
    IDENTITY_P = 441,
    IF_P = 442,
    ILIKE = 443,
    IMMEDIATE = 444,
    IMMUTABLE = 445,
    IMPLICIT_P = 446,
    IMPORT_P = 447,
    IN_P = 448,
    INCLUDING = 449,
    INCREMENT = 450,
    INDEX = 451,
    INDEXES = 452,
    INHERIT = 453,
    INHERITS = 454,
    INITIALLY = 455,
    INLINE_P = 456,
    INNER_P = 457,
    INOUT = 458,
    INPUT_P = 459,
    INSENSITIVE = 460,
    INSERT = 461,
    INSTEAD = 462,
    INT_P = 463,
    INTEGER = 464,
    INTERSECT = 465,
    INTERVAL = 466,
    INTO = 467,
    INVOKER = 468,
    IS = 469,
    ISNULL = 470,
    ISOLATION = 471,
    JOIN = 472,
    KEY = 473,
    LABEL = 474,
    LANGUAGE = 475,
    LARGE_P = 476,
    LAST_P = 477,
    LATERAL_P = 478,
    LEADING = 479,
    LEAKPROOF = 480,
    LEFT = 481,
    LEVEL = 482,
    LIKE = 483,
    LIMIT = 484,
    LISTEN = 485,
    LOAD = 486,
    LOCAL = 487,
    LOCALTIME = 488,
    LOCALTIMESTAMP = 489,
    LOCATION = 490,
    LOCK_P = 491,
    LOCKED = 492,
    LOGGED = 493,
    MACRO = 494,
    MAP = 495,
    MAPPING = 496,
    MATCH = 497,
    MATERIALIZED = 498,
    MAXVALUE = 499,
    METHOD = 500,
    MICROSECOND_P = 501,
    MICROSECONDS_P = 502,
    MILLISECOND_P = 503,
    MILLISECONDS_P = 504,
    MINUTE_P = 505,
    MINUTES_P = 506,
    MINVALUE = 507,
    MODE = 508,
    MONTH_P = 509,
    MONTHS_P = 510,
    MOVE = 511,
    NAME_P = 512,
    NAMES = 513,
    NATIONAL = 514,
    NATURAL = 515,
    NCHAR = 516,
    NEW = 517,
    NEXT = 518,
    NO = 519,
    NONE = 520,
    NOT = 521,
    NOTHING = 522,
    NOTIFY = 523,
    NOTNULL = 524,
    NOWAIT = 525,
    NULL_P = 526,
    NULLIF = 527,
    NULLS_P = 528,
    NUMERIC = 529,
    OBJECT_P = 530,
    OF = 531,
    OFF = 532,
    OFFSET = 533,
    OIDS = 534,
    OLD = 535,
    ON = 536,
    ONLY = 537,
    OPERATOR = 538,
    OPTION = 539,
    OPTIONS = 540,
    OR = 541,
    ORDER = 542,
    ORDINALITY = 543,
    OUT_P = 544,
    OUTER_P = 545,
    OVER = 546,
    OVERLAPS = 547,
    OVERLAY = 548,
    OVERRIDING = 549,
    OWNED = 550,
    OWNER = 551,
    PARALLEL = 552,
    PARSER = 553,
    PARTIAL = 554,
    PARTITION = 555,
    PASSING = 556,
    PASSWORD = 557,
    PERCENT = 558,
    PLACING = 559,
    PLANS = 560,
    POLICY = 561,
    POSITION = 562,
    PRAGMA_P = 563,
    PRECEDING = 564,
    PRECISION = 565,
    PREPARE = 566,
    PREPARED = 567,
    PRESERVE = 568,
    PRIMARY = 569,
    PRIOR = 570,
    PRIVILEGES = 571,
    PROCEDURAL = 572,
    PROCEDURE = 573,
    PROGRAM = 574,
    PUBLICATION = 575,
    QUOTE = 576,
    RANGE = 577,
    READ_P = 578,
    REAL = 579,
    REASSIGN = 580,
    RECHECK = 581,
    RECURSIVE = 582,
    REF = 583,
    REFERENCES = 584,
    REFERENCING = 585,
    REFRESH = 586,
    REINDEX = 587,
    RELATIVE_P = 588,
    RELEASE = 589,
    RENAME = 590,
    REPEATABLE = 591,
    REPLACE = 592,
    REPLICA = 593,
    RESET = 594,
    RESTART = 595,
    RESTRICT = 596,
    RETURNING = 597,
    RETURNS = 598,
    REVOKE = 599,
    RIGHT = 600,
    ROLE = 601,
    ROLLBACK = 602,
    ROLLUP = 603,
    ROW = 604,
    ROWS = 605,
    RULE = 606,
    SAMPLE = 607,
    SAVEPOINT = 608,
    SCHEMA = 609,
    SCHEMAS = 610,
    SCROLL = 611,
    SEARCH = 612,
    SECOND_P = 613,
    SECONDS_P = 614,
    SECURITY = 615,
    SELECT = 616,
    SEQUENCE = 617,
    SEQUENCES = 618,
    SERIALIZABLE = 619,
    SERVER = 620,
    SESSION = 621,
    SESSION_USER = 622,
    SET = 623,
    SETOF = 624,
    SETS = 625,
    SHARE = 626,
    SHOW = 627,
    SIMILAR = 628,
    SIMPLE = 629,
    SKIP = 630,
    SMALLINT = 631,
    SNAPSHOT = 632,
    SOME = 633,
    SQL_P = 634,
    STABLE = 635,
    STANDALONE_P = 636,
    START = 637,
    STATEMENT = 638,
    STATISTICS = 639,
    STDIN = 640,
    STDOUT = 641,
    STORAGE = 642,
    STRICT_P = 643,
    STRIP_P = 644,
    STRUCT = 645,
    SUBSCRIPTION = 646,
    SUBSTRING = 647,
    SYMMETRIC = 648,
    SYSID = 649,
    SYSTEM_P = 650,
    TABLE = 651,
    TABLES = 652,
    TABLESAMPLE = 653,
    TABLESPACE = 654,
    TEMP = 655,
    TEMPLATE = 656,
    TEMPORARY = 657,
    TEXT_P = 658,
    THEN = 659,
    TIME = 660,
    TIMESTAMP = 661,
    TO = 662,
    TRAILING = 663,
    TRANSACTION = 664,
    TRANSFORM = 665,
    TREAT = 666,
    TRIGGER = 667,
    TRIM = 668,
    TRUE_P = 669,
    TRUNCATE = 670,
    TRUSTED = 671,
    TRY_CAST = 672,
    TYPE_P = 673,
    TYPES_P = 674,
    UNBOUNDED = 675,
    UNCOMMITTED = 676,
    UNENCRYPTED = 677,
    UNION = 678,
    UNIQUE = 679,
    UNKNOWN = 680,
    UNLISTEN = 681,
    UNLOGGED = 682,
    UNTIL = 683,
    UPDATE = 684,
    USER = 685,
    USING = 686,
    VACUUM = 687,
    VALID = 688,
    VALIDATE = 689,
    VALIDATOR = 690,
    VALUE_P = 691,
    VALUES = 692,
    VARCHAR = 693,
    VARIADIC = 694,
    VARYING = 695,
    VERBOSE = 696,
    VERSION_P = 697,
    VERTEX = 698,
    VIEW = 699,
    VIEWS = 700,
    VOLATILE = 701,
    WHEN = 702,
    WHERE = 703,
    WHITESPACE_P = 704,
    WINDOW = 705,
    WITH = 706,
    WITHIN = 707,
    WITHOUT = 708,
    WORK = 709,
    WRAPPER = 710,
    WRITE_P = 711,
    XML_P = 712,
    XMLATTRIBUTES = 713,
    XMLCONCAT = 714,
    XMLELEMENT = 715,
    XMLEXISTS = 716,
    XMLFOREST = 717,
    XMLNAMESPACES = 718,
    XMLPARSE = 719,
    XMLPI = 720,
    XMLROOT = 721,
    XMLSERIALIZE = 722,
    XMLTABLE = 723,
    YEAR_P = 724,
    YEARS_P = 725,
    YES_P = 726,
    ZONE = 727,
    NOT_LA = 728,
    NULLS_LA = 729,
    WITH_LA = 730,
    POSTFIXOP = 731,
    UMINUS = 732
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED

union YYSTYPE
{
#line 14 "third_party/libpg_query/grammar/grammar.y" /* yacc.c:1909  */

	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;
	const char          *conststr;

	char				chr;
	bool				boolean;
	PGJoinType			jtype;
	PGDropBehavior		dbehavior;
	PGOnCommitAction		oncommit;
	PGList				*list;
	PGNode				*node;
	PGValue				*value;
	PGObjectType			objtype;
	PGTypeName			*typnam;
	PGObjectWithArgs		*objwithargs;
	PGDefElem				*defelt;
	PGSortBy				*sortby;
	PGWindowDef			*windef;
	PGJoinExpr			*jexpr;
	PGIndexElem			*ielem;
	PGAlias				*alias;
	PGRangeVar			*range;
	PGIntoClause			*into;
	PGWithClause			*with;
	PGInferClause			*infer;
	PGOnConflictClause	*onconflict;
	PGAIndices			*aind;
	PGResTarget			*target;
	PGInsertStmt			*istmt;
	PGVariableSetStmt		*vsetstmt;
	PGOverridingKind       override;
	PGSortByDir            sortorder;
	PGSortByNulls          nullorder;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;

#line 575 "third_party/libpg_query/grammar/grammar_out.hpp" /* yacc.c:1909  */
};

typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif



int base_yyparse (core_yyscan_t yyscanner);

#endif /* !YY_BASE_YY_THIRD_PARTY_LIBPG_QUERY_GRAMMAR_GRAMMAR_OUT_HPP_INCLUDED  */
