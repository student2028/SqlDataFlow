package org.student.spark.sql;// Generated from DSLSQL.g4 by ANTLR 4.7.1

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNDeserializer;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class DSLSQLParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, AS=11, INTO=12, LOAD=13, SAVE=14, SELECT=15, INSERT=16, CREATE=17, 
		DROP=18, REFRESH=19, SET=20, CONNECT=21, TRAIN=22, RUN=23, PREDICT=24, 
		REGISTER=25, UNREGISTER=26, INCLUDE=27, OPTIONS=28, WHERE=29, PARTITIONBY=30, 
		OVERWRITE=31, APPEND=32, ERRORIfExists=33, IGNORE=34, STRING=35, BLOCK_STRING=36, 
		IDENTIFIER=37, BACKQUOTED_IDENTIFIER=38, EXECUTE_COMMAND=39, EXECUTE_TOKEN=40, 
		SIMPLE_COMMENT=41, BRACKETED_EMPTY_COMMENT=42, BRACKETED_COMMENT=43, WS=44, 
		UNRECOGNIZED=45;
	public static final int
		RULE_statement = 0, RULE_sql = 1, RULE_as = 2, RULE_into = 3, RULE_saveMode = 4, 
		RULE_where = 5, RULE_whereExpressions = 6, RULE_overwrite = 7, RULE_append = 8, 
		RULE_errorIfExists = 9, RULE_ignore = 10, RULE_booleanExpression = 11, 
		RULE_expression = 12, RULE_ender = 13, RULE_format = 14, RULE_path = 15, 
		RULE_commandValue = 16, RULE_rawCommandValue = 17, RULE_setValue = 18, 
		RULE_setKey = 19, RULE_db = 20, RULE_asTableName = 21, RULE_tableName = 22, 
		RULE_functionName = 23, RULE_colGroup = 24, RULE_col = 25, RULE_qualifiedName = 26, 
		RULE_identifier = 27, RULE_strictIdentifier = 28, RULE_quotedIdentifier = 29;
	public static final String[] ruleNames = {
		"statement", "sql", "as", "into", "saveMode", "where", "whereExpressions", 
		"overwrite", "append", "errorIfExists", "ignore", "booleanExpression", 
		"expression", "ender", "format", "path", "commandValue", "rawCommandValue", 
		"setValue", "setKey", "db", "asTableName", "tableName", "functionName", 
		"colGroup", "col", "qualifiedName", "identifier", "strictIdentifier", 
		"quotedIdentifier"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'.'", "';'", "'='", "'and'", "'-'", "'/'", "'>'", "'<'", "'~'", 
		"','", "'as'", "'into'", "'load'", "'save'", "'select'", "'insert'", "'create'", 
		"'drop'", "'refresh'", "'set'", "'connect'", "'train'", "'run'", "'predict'", 
		"'register'", "'unregister'", "'include'", "'options'", "'where'", null, 
		"'overwrite'", "'append'", "'errorIfExists'", "'ignore'", null, null, 
		null, null, null, "'!'", null, "'/**/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, "AS", 
		"INTO", "LOAD", "SAVE", "SELECT", "INSERT", "CREATE", "DROP", "REFRESH", 
		"SET", "CONNECT", "TRAIN", "RUN", "PREDICT", "REGISTER", "UNREGISTER", 
		"INCLUDE", "OPTIONS", "WHERE", "PARTITIONBY", "OVERWRITE", "APPEND", "ERRORIfExists", 
		"IGNORE", "STRING", "BLOCK_STRING", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", 
		"EXECUTE_COMMAND", "EXECUTE_TOKEN", "SIMPLE_COMMENT", "BRACKETED_EMPTY_COMMENT", 
		"BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "DSLSQL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public DSLSQLParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class StatementContext extends ParserRuleContext {
		public List<SqlContext> sql() {
			return getRuleContexts(SqlContext.class);
		}
		public SqlContext sql(int i) {
			return getRuleContext(SqlContext.class,i);
		}
		public List<EnderContext> ender() {
			return getRuleContexts(EnderContext.class);
		}
		public EnderContext ender(int i) {
			return getRuleContext(EnderContext.class,i);
		}
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(65);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LOAD) | (1L << SAVE) | (1L << SELECT) | (1L << INSERT) | (1L << CREATE) | (1L << DROP) | (1L << REFRESH) | (1L << SET) | (1L << CONNECT) | (1L << TRAIN) | (1L << RUN) | (1L << PREDICT) | (1L << REGISTER) | (1L << UNREGISTER) | (1L << INCLUDE) | (1L << EXECUTE_COMMAND) | (1L << SIMPLE_COMMENT))) != 0)) {
				{
				{
				setState(60);
				sql();
				setState(61);
				ender();
				}
				}
				setState(67);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SqlContext extends ParserRuleContext {
		public TerminalNode LOAD() { return getToken(DSLSQLParser.LOAD, 0); }
		public FormatContext format() {
			return getRuleContext(FormatContext.class,0);
		}
		public PathContext path() {
			return getRuleContext(PathContext.class,0);
		}
		public AsContext as() {
			return getRuleContext(AsContext.class,0);
		}
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public WhereContext where() {
			return getRuleContext(WhereContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode SAVE() { return getToken(DSLSQLParser.SAVE, 0); }
		public SaveModeContext saveMode() {
			return getRuleContext(SaveModeContext.class,0);
		}
		public TerminalNode PARTITIONBY() { return getToken(DSLSQLParser.PARTITIONBY, 0); }
		public ColContext col() {
			return getRuleContext(ColContext.class,0);
		}
		public List<ColGroupContext> colGroup() {
			return getRuleContexts(ColGroupContext.class);
		}
		public ColGroupContext colGroup(int i) {
			return getRuleContext(ColGroupContext.class,i);
		}
		public TerminalNode SELECT() { return getToken(DSLSQLParser.SELECT, 0); }
		public TerminalNode INSERT() { return getToken(DSLSQLParser.INSERT, 0); }
		public TerminalNode CREATE() { return getToken(DSLSQLParser.CREATE, 0); }
		public TerminalNode DROP() { return getToken(DSLSQLParser.DROP, 0); }
		public TerminalNode REFRESH() { return getToken(DSLSQLParser.REFRESH, 0); }
		public TerminalNode SET() { return getToken(DSLSQLParser.SET, 0); }
		public SetKeyContext setKey() {
			return getRuleContext(SetKeyContext.class,0);
		}
		public SetValueContext setValue() {
			return getRuleContext(SetValueContext.class,0);
		}
		public TerminalNode CONNECT() { return getToken(DSLSQLParser.CONNECT, 0); }
		public DbContext db() {
			return getRuleContext(DbContext.class,0);
		}
		public TerminalNode TRAIN() { return getToken(DSLSQLParser.TRAIN, 0); }
		public TerminalNode RUN() { return getToken(DSLSQLParser.RUN, 0); }
		public TerminalNode PREDICT() { return getToken(DSLSQLParser.PREDICT, 0); }
		public IntoContext into() {
			return getRuleContext(IntoContext.class,0);
		}
		public List<AsTableNameContext> asTableName() {
			return getRuleContexts(AsTableNameContext.class);
		}
		public AsTableNameContext asTableName(int i) {
			return getRuleContext(AsTableNameContext.class,i);
		}
		public TerminalNode REGISTER() { return getToken(DSLSQLParser.REGISTER, 0); }
		public FunctionNameContext functionName() {
			return getRuleContext(FunctionNameContext.class,0);
		}
		public TerminalNode UNREGISTER() { return getToken(DSLSQLParser.UNREGISTER, 0); }
		public TerminalNode INCLUDE() { return getToken(DSLSQLParser.INCLUDE, 0); }
		public TerminalNode EXECUTE_COMMAND() { return getToken(DSLSQLParser.EXECUTE_COMMAND, 0); }
		public List<CommandValueContext> commandValue() {
			return getRuleContexts(CommandValueContext.class);
		}
		public CommandValueContext commandValue(int i) {
			return getRuleContext(CommandValueContext.class,i);
		}
		public List<RawCommandValueContext> rawCommandValue() {
			return getRuleContexts(RawCommandValueContext.class);
		}
		public RawCommandValueContext rawCommandValue(int i) {
			return getRuleContext(RawCommandValueContext.class,i);
		}
		public TerminalNode SIMPLE_COMMENT() { return getToken(DSLSQLParser.SIMPLE_COMMENT, 0); }
		public SqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sql; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitSql(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SqlContext sql() throws RecognitionException {
		SqlContext _localctx = new SqlContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_sql);
		int _la;
		try {
			int _alt;
			setState(283);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LOAD:
				enterOuterAlt(_localctx, 1);
				{
				setState(68);
				match(LOAD);
				setState(69);
				format();
				setState(70);
				match(T__0);
				setState(71);
				path();
				setState(73);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS || _la==WHERE) {
					{
					setState(72);
					where();
					}
				}

				setState(76);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(75);
					expression();
					}
				}

				setState(81);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(78);
					booleanExpression();
					}
					}
					setState(83);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(84);
				as();
				setState(85);
				tableName();
				}
				break;
			case SAVE:
				enterOuterAlt(_localctx, 2);
				{
				setState(87);
				match(SAVE);
				setState(89);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << OVERWRITE) | (1L << APPEND) | (1L << ERRORIfExists) | (1L << IGNORE))) != 0)) {
					{
					setState(88);
					saveMode();
					}
				}

				setState(91);
				tableName();
				setState(92);
				as();
				setState(93);
				format();
				setState(95);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(94);
					match(T__0);
					}
				}

				setState(98);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
				case 1:
					{
					setState(97);
					path();
					}
					break;
				}
				setState(101);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS || _la==WHERE) {
					{
					setState(100);
					where();
					}
				}

				setState(104);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(103);
					expression();
					}
				}

				setState(109);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(106);
					booleanExpression();
					}
					}
					setState(111);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(122);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITIONBY) {
					{
					setState(112);
					match(PARTITIONBY);
					setState(114);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
						{
						setState(113);
						col();
						}
					}

					setState(119);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__9) {
						{
						{
						setState(116);
						colGroup();
						}
						}
						setState(121);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case SELECT:
				enterOuterAlt(_localctx, 3);
				{
				setState(124);
				match(SELECT);
				setState(128);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,13,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(125);
						_la = _input.LA(1);
						if ( _la <= 0 || (_la==T__1) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)== Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						}
						}
					}
					setState(130);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,13,_ctx);
				}
				setState(131);
				as();
				setState(132);
				tableName();
				}
				break;
			case INSERT:
				enterOuterAlt(_localctx, 4);
				{
				setState(134);
				match(INSERT);
				setState(138);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__2) | (1L << T__3) | (1L << T__4) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << AS) | (1L << INTO) | (1L << LOAD) | (1L << SAVE) | (1L << SELECT) | (1L << INSERT) | (1L << CREATE) | (1L << DROP) | (1L << REFRESH) | (1L << SET) | (1L << CONNECT) | (1L << TRAIN) | (1L << RUN) | (1L << PREDICT) | (1L << REGISTER) | (1L << UNREGISTER) | (1L << INCLUDE) | (1L << OPTIONS) | (1L << WHERE) | (1L << PARTITIONBY) | (1L << OVERWRITE) | (1L << APPEND) | (1L << ERRORIfExists) | (1L << IGNORE) | (1L << STRING) | (1L << BLOCK_STRING) | (1L << IDENTIFIER) | (1L << BACKQUOTED_IDENTIFIER) | (1L << EXECUTE_COMMAND) | (1L << EXECUTE_TOKEN) | (1L << SIMPLE_COMMENT) | (1L << BRACKETED_EMPTY_COMMENT) | (1L << BRACKETED_COMMENT) | (1L << WS) | (1L << UNRECOGNIZED))) != 0)) {
					{
					{
					setState(135);
					_la = _input.LA(1);
					if ( _la <= 0 || (_la==T__1) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)== Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(140);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case CREATE:
				enterOuterAlt(_localctx, 5);
				{
				setState(141);
				match(CREATE);
				setState(145);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__2) | (1L << T__3) | (1L << T__4) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << AS) | (1L << INTO) | (1L << LOAD) | (1L << SAVE) | (1L << SELECT) | (1L << INSERT) | (1L << CREATE) | (1L << DROP) | (1L << REFRESH) | (1L << SET) | (1L << CONNECT) | (1L << TRAIN) | (1L << RUN) | (1L << PREDICT) | (1L << REGISTER) | (1L << UNREGISTER) | (1L << INCLUDE) | (1L << OPTIONS) | (1L << WHERE) | (1L << PARTITIONBY) | (1L << OVERWRITE) | (1L << APPEND) | (1L << ERRORIfExists) | (1L << IGNORE) | (1L << STRING) | (1L << BLOCK_STRING) | (1L << IDENTIFIER) | (1L << BACKQUOTED_IDENTIFIER) | (1L << EXECUTE_COMMAND) | (1L << EXECUTE_TOKEN) | (1L << SIMPLE_COMMENT) | (1L << BRACKETED_EMPTY_COMMENT) | (1L << BRACKETED_COMMENT) | (1L << WS) | (1L << UNRECOGNIZED))) != 0)) {
					{
					{
					setState(142);
					_la = _input.LA(1);
					if ( _la <= 0 || (_la==T__1) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)== Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(147);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case DROP:
				enterOuterAlt(_localctx, 6);
				{
				setState(148);
				match(DROP);
				setState(152);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__2) | (1L << T__3) | (1L << T__4) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << AS) | (1L << INTO) | (1L << LOAD) | (1L << SAVE) | (1L << SELECT) | (1L << INSERT) | (1L << CREATE) | (1L << DROP) | (1L << REFRESH) | (1L << SET) | (1L << CONNECT) | (1L << TRAIN) | (1L << RUN) | (1L << PREDICT) | (1L << REGISTER) | (1L << UNREGISTER) | (1L << INCLUDE) | (1L << OPTIONS) | (1L << WHERE) | (1L << PARTITIONBY) | (1L << OVERWRITE) | (1L << APPEND) | (1L << ERRORIfExists) | (1L << IGNORE) | (1L << STRING) | (1L << BLOCK_STRING) | (1L << IDENTIFIER) | (1L << BACKQUOTED_IDENTIFIER) | (1L << EXECUTE_COMMAND) | (1L << EXECUTE_TOKEN) | (1L << SIMPLE_COMMENT) | (1L << BRACKETED_EMPTY_COMMENT) | (1L << BRACKETED_COMMENT) | (1L << WS) | (1L << UNRECOGNIZED))) != 0)) {
					{
					{
					setState(149);
					_la = _input.LA(1);
					if ( _la <= 0 || (_la==T__1) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)== Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(154);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case REFRESH:
				enterOuterAlt(_localctx, 7);
				{
				setState(155);
				match(REFRESH);
				setState(159);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__2) | (1L << T__3) | (1L << T__4) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << AS) | (1L << INTO) | (1L << LOAD) | (1L << SAVE) | (1L << SELECT) | (1L << INSERT) | (1L << CREATE) | (1L << DROP) | (1L << REFRESH) | (1L << SET) | (1L << CONNECT) | (1L << TRAIN) | (1L << RUN) | (1L << PREDICT) | (1L << REGISTER) | (1L << UNREGISTER) | (1L << INCLUDE) | (1L << OPTIONS) | (1L << WHERE) | (1L << PARTITIONBY) | (1L << OVERWRITE) | (1L << APPEND) | (1L << ERRORIfExists) | (1L << IGNORE) | (1L << STRING) | (1L << BLOCK_STRING) | (1L << IDENTIFIER) | (1L << BACKQUOTED_IDENTIFIER) | (1L << EXECUTE_COMMAND) | (1L << EXECUTE_TOKEN) | (1L << SIMPLE_COMMENT) | (1L << BRACKETED_EMPTY_COMMENT) | (1L << BRACKETED_COMMENT) | (1L << WS) | (1L << UNRECOGNIZED))) != 0)) {
					{
					{
					setState(156);
					_la = _input.LA(1);
					if ( _la <= 0 || (_la==T__1) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)== Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(161);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case SET:
				enterOuterAlt(_localctx, 8);
				{
				setState(162);
				match(SET);
				setState(163);
				setKey();
				setState(164);
				match(T__2);
				setState(165);
				setValue();
				setState(167);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS || _la==WHERE) {
					{
					setState(166);
					where();
					}
				}

				setState(170);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(169);
					expression();
					}
				}

				setState(175);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(172);
					booleanExpression();
					}
					}
					setState(177);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case CONNECT:
				enterOuterAlt(_localctx, 9);
				{
				setState(178);
				match(CONNECT);
				setState(179);
				format();
				setState(181);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS || _la==WHERE) {
					{
					setState(180);
					where();
					}
				}

				setState(184);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(183);
					expression();
					}
				}

				setState(189);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(186);
					booleanExpression();
					}
					}
					setState(191);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(195);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(192);
					as();
					setState(193);
					db();
					}
				}

				}
				break;
			case TRAIN:
			case RUN:
			case PREDICT:
				enterOuterAlt(_localctx, 10);
				{
				setState(197);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << TRAIN) | (1L << RUN) | (1L << PREDICT))) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)== Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(198);
				tableName();
				setState(201);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case AS:
					{
					setState(199);
					as();
					}
					break;
				case INTO:
					{
					setState(200);
					into();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(203);
				format();
				setState(204);
				match(T__0);
				setState(205);
				path();
				setState(207);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS || _la==WHERE) {
					{
					setState(206);
					where();
					}
				}

				setState(210);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(209);
					expression();
					}
				}

				setState(215);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(212);
					booleanExpression();
					}
					}
					setState(217);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(221);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==AS) {
					{
					{
					setState(218);
					asTableName();
					}
					}
					setState(223);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case REGISTER:
				enterOuterAlt(_localctx, 11);
				{
				setState(224);
				match(REGISTER);
				setState(225);
				format();
				setState(226);
				match(T__0);
				setState(227);
				path();
				setState(228);
				as();
				setState(229);
				functionName();
				setState(231);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS || _la==WHERE) {
					{
					setState(230);
					where();
					}
				}

				setState(234);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(233);
					expression();
					}
				}

				setState(239);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(236);
					booleanExpression();
					}
					}
					setState(241);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case UNREGISTER:
				enterOuterAlt(_localctx, 12);
				{
				setState(242);
				match(UNREGISTER);
				setState(243);
				format();
				setState(244);
				match(T__0);
				setState(245);
				path();
				setState(247);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS || _la==WHERE) {
					{
					setState(246);
					where();
					}
				}

				setState(250);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(249);
					expression();
					}
				}

				setState(255);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(252);
					booleanExpression();
					}
					}
					setState(257);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case INCLUDE:
				enterOuterAlt(_localctx, 13);
				{
				setState(258);
				match(INCLUDE);
				setState(259);
				format();
				setState(260);
				match(T__0);
				setState(261);
				path();
				setState(263);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS || _la==WHERE) {
					{
					setState(262);
					where();
					}
				}

				setState(266);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(265);
					expression();
					}
				}

				setState(271);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(268);
					booleanExpression();
					}
					}
					setState(273);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case EXECUTE_COMMAND:
				enterOuterAlt(_localctx, 14);
				{
				setState(274);
				match(EXECUTE_COMMAND);
				setState(279);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__4) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << STRING) | (1L << BLOCK_STRING) | (1L << IDENTIFIER) | (1L << BACKQUOTED_IDENTIFIER))) != 0)) {
					{
					setState(277);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
					case 1:
						{
						setState(275);
						commandValue();
						}
						break;
					case 2:
						{
						setState(276);
						rawCommandValue();
						}
						break;
					}
					}
					setState(281);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case SIMPLE_COMMENT:
				enterOuterAlt(_localctx, 15);
				{
				setState(282);
				match(SIMPLE_COMMENT);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AsContext extends ParserRuleContext {
		public TerminalNode AS() { return getToken(DSLSQLParser.AS, 0); }
		public AsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_as; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitAs(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AsContext as() throws RecognitionException {
		AsContext _localctx = new AsContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_as);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(285);
			match(AS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntoContext extends ParserRuleContext {
		public TerminalNode INTO() { return getToken(DSLSQLParser.INTO, 0); }
		public IntoContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_into; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitInto(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntoContext into() throws RecognitionException {
		IntoContext _localctx = new IntoContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_into);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(287);
			match(INTO);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SaveModeContext extends ParserRuleContext {
		public TerminalNode OVERWRITE() { return getToken(DSLSQLParser.OVERWRITE, 0); }
		public TerminalNode APPEND() { return getToken(DSLSQLParser.APPEND, 0); }
		public TerminalNode ERRORIfExists() { return getToken(DSLSQLParser.ERRORIfExists, 0); }
		public TerminalNode IGNORE() { return getToken(DSLSQLParser.IGNORE, 0); }
		public SaveModeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_saveMode; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitSaveMode(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SaveModeContext saveMode() throws RecognitionException {
		SaveModeContext _localctx = new SaveModeContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_saveMode);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(289);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << OVERWRITE) | (1L << APPEND) | (1L << ERRORIfExists) | (1L << IGNORE))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)== Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WhereContext extends ParserRuleContext {
		public TerminalNode OPTIONS() { return getToken(DSLSQLParser.OPTIONS, 0); }
		public TerminalNode WHERE() { return getToken(DSLSQLParser.WHERE, 0); }
		public WhereContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_where; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitWhere(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhereContext where() throws RecognitionException {
		WhereContext _localctx = new WhereContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_where);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(291);
			_la = _input.LA(1);
			if ( !(_la==OPTIONS || _la==WHERE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)== Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WhereExpressionsContext extends ParserRuleContext {
		public WhereContext where() {
			return getRuleContext(WhereContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public WhereExpressionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whereExpressions; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitWhereExpressions(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhereExpressionsContext whereExpressions() throws RecognitionException {
		WhereExpressionsContext _localctx = new WhereExpressionsContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_whereExpressions);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(293);
			where();
			setState(295);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
				{
				setState(294);
				expression();
				}
			}

			setState(300);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(297);
				booleanExpression();
				}
				}
				setState(302);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OverwriteContext extends ParserRuleContext {
		public OverwriteContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_overwrite; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitOverwrite(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OverwriteContext overwrite() throws RecognitionException {
		OverwriteContext _localctx = new OverwriteContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_overwrite);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(303);
			match(OVERWRITE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AppendContext extends ParserRuleContext {
		public AppendContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_append; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitAppend(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AppendContext append() throws RecognitionException {
		AppendContext _localctx = new AppendContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_append);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(305);
			match(APPEND);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ErrorIfExistsContext extends ParserRuleContext {
		public ErrorIfExistsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_errorIfExists; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitErrorIfExists(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ErrorIfExistsContext errorIfExists() throws RecognitionException {
		ErrorIfExistsContext _localctx = new ErrorIfExistsContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_errorIfExists);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(307);
			match(ERRORIfExists);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IgnoreContext extends ParserRuleContext {
		public IgnoreContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ignore; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitIgnore(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IgnoreContext ignore() throws RecognitionException {
		IgnoreContext _localctx = new IgnoreContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_ignore);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(309);
			match(IGNORE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitBooleanExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_booleanExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(311);
			match(T__3);
			setState(312);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode STRING() { return getToken(DSLSQLParser.STRING, 0); }
		public TerminalNode BLOCK_STRING() { return getToken(DSLSQLParser.BLOCK_STRING, 0); }
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_expression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(314);
			qualifiedName();
			setState(315);
			match(T__2);
			setState(316);
			_la = _input.LA(1);
			if ( !(_la==STRING || _la==BLOCK_STRING) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)== Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class EnderContext extends ParserRuleContext {
		public EnderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ender; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitEnder(this);
			else return visitor.visitChildren(this);
		}
	}

	public final EnderContext ender() throws RecognitionException {
		EnderContext _localctx = new EnderContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_ender);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(318);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FormatContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public FormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_format; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitFormat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FormatContext format() throws RecognitionException {
		FormatContext _localctx = new FormatContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_format);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(320);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PathContext extends ParserRuleContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public PathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_path; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitPath(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PathContext path() throws RecognitionException {
		PathContext _localctx = new PathContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_path);
		try {
			setState(324);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,44,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(322);
				quotedIdentifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(323);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CommandValueContext extends ParserRuleContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(DSLSQLParser.STRING, 0); }
		public TerminalNode BLOCK_STRING() { return getToken(DSLSQLParser.BLOCK_STRING, 0); }
		public CommandValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_commandValue; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitCommandValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CommandValueContext commandValue() throws RecognitionException {
		CommandValueContext _localctx = new CommandValueContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_commandValue);
		try {
			setState(329);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(326);
				quotedIdentifier();
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(327);
				match(STRING);
				}
				break;
			case BLOCK_STRING:
				enterOuterAlt(_localctx, 3);
				{
				setState(328);
				match(BLOCK_STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RawCommandValueContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public RawCommandValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rawCommandValue; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitRawCommandValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RawCommandValueContext rawCommandValue() throws RecognitionException {
		RawCommandValueContext _localctx = new RawCommandValueContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_rawCommandValue);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(338);
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					setState(338);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case IDENTIFIER:
					case BACKQUOTED_IDENTIFIER:
						{
						setState(331);
						identifier();
						}
						break;
					case T__4:
						{
						setState(332);
						match(T__4);
						}
						break;
					case T__5:
						{
						setState(333);
						match(T__5);
						}
						break;
					case T__6:
						{
						setState(334);
						match(T__6);
						}
						break;
					case T__7:
						{
						setState(335);
						match(T__7);
						}
						break;
					case T__0:
						{
						setState(336);
						match(T__0);
						}
						break;
					case T__8:
						{
						setState(337);
						match(T__8);
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(340);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,47,_ctx);
			} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetValueContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(DSLSQLParser.STRING, 0); }
		public TerminalNode BLOCK_STRING() { return getToken(DSLSQLParser.BLOCK_STRING, 0); }
		public SetValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setValue; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitSetValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetValueContext setValue() throws RecognitionException {
		SetValueContext _localctx = new SetValueContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_setValue);
		try {
			setState(346);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(342);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(343);
				quotedIdentifier();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(344);
				match(STRING);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(345);
				match(BLOCK_STRING);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetKeyContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public SetKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setKey; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitSetKey(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetKeyContext setKey() throws RecognitionException {
		SetKeyContext _localctx = new SetKeyContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_setKey);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(348);
			qualifiedName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DbContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DbContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_db; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitDb(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DbContext db() throws RecognitionException {
		DbContext _localctx = new DbContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_db);
		try {
			setState(352);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,49,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(350);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(351);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AsTableNameContext extends ParserRuleContext {
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public AsTableNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_asTableName; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitAsTableName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AsTableNameContext asTableName() throws RecognitionException {
		AsTableNameContext _localctx = new AsTableNameContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_asTableName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(354);
			match(AS);
			setState(355);
			tableName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableNameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TableNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableName; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitTableName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableNameContext tableName() throws RecognitionException {
		TableNameContext _localctx = new TableNameContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_tableName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(357);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionNameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public FunctionNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionName; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitFunctionName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionNameContext functionName() throws RecognitionException {
		FunctionNameContext _localctx = new FunctionNameContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_functionName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(359);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColGroupContext extends ParserRuleContext {
		public ColContext col() {
			return getRuleContext(ColContext.class,0);
		}
		public ColGroupContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colGroup; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitColGroup(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColGroupContext colGroup() throws RecognitionException {
		ColGroupContext _localctx = new ColGroupContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_colGroup);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(361);
			match(T__9);
			setState(362);
			col();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_col; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitCol(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColContext col() throws RecognitionException {
		ColContext _localctx = new ColContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_col);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(364);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitQualifiedName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_qualifiedName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(366);
			identifier();
			setState(371);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__0) {
				{
				{
				setState(367);
				match(T__0);
				setState(368);
				identifier();
				}
				}
				setState(373);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_identifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(374);
			strictIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StrictIdentifierContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(DSLSQLParser.IDENTIFIER, 0); }
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public StrictIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strictIdentifier; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitStrictIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StrictIdentifierContext strictIdentifier() throws RecognitionException {
		StrictIdentifierContext _localctx = new StrictIdentifierContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_strictIdentifier);
		try {
			setState(378);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(376);
				match(IDENTIFIER);
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(377);
				quotedIdentifier();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuotedIdentifierContext extends ParserRuleContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(DSLSQLParser.BACKQUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedIdentifier; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DSLSQLVisitor ) return ((DSLSQLVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
		QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_quotedIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(380);
			match(BACKQUOTED_IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3/\u0181\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\3\2\3\2\3"+
		"\2\7\2B\n\2\f\2\16\2E\13\2\3\3\3\3\3\3\3\3\3\3\5\3L\n\3\3\3\5\3O\n\3\3"+
		"\3\7\3R\n\3\f\3\16\3U\13\3\3\3\3\3\3\3\3\3\3\3\5\3\\\n\3\3\3\3\3\3\3\3"+
		"\3\5\3b\n\3\3\3\5\3e\n\3\3\3\5\3h\n\3\3\3\5\3k\n\3\3\3\7\3n\n\3\f\3\16"+
		"\3q\13\3\3\3\3\3\5\3u\n\3\3\3\7\3x\n\3\f\3\16\3{\13\3\5\3}\n\3\3\3\3\3"+
		"\7\3\u0081\n\3\f\3\16\3\u0084\13\3\3\3\3\3\3\3\3\3\3\3\7\3\u008b\n\3\f"+
		"\3\16\3\u008e\13\3\3\3\3\3\7\3\u0092\n\3\f\3\16\3\u0095\13\3\3\3\3\3\7"+
		"\3\u0099\n\3\f\3\16\3\u009c\13\3\3\3\3\3\7\3\u00a0\n\3\f\3\16\3\u00a3"+
		"\13\3\3\3\3\3\3\3\3\3\3\3\5\3\u00aa\n\3\3\3\5\3\u00ad\n\3\3\3\7\3\u00b0"+
		"\n\3\f\3\16\3\u00b3\13\3\3\3\3\3\3\3\5\3\u00b8\n\3\3\3\5\3\u00bb\n\3\3"+
		"\3\7\3\u00be\n\3\f\3\16\3\u00c1\13\3\3\3\3\3\3\3\5\3\u00c6\n\3\3\3\3\3"+
		"\3\3\3\3\5\3\u00cc\n\3\3\3\3\3\3\3\3\3\5\3\u00d2\n\3\3\3\5\3\u00d5\n\3"+
		"\3\3\7\3\u00d8\n\3\f\3\16\3\u00db\13\3\3\3\7\3\u00de\n\3\f\3\16\3\u00e1"+
		"\13\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3\u00ea\n\3\3\3\5\3\u00ed\n\3\3\3"+
		"\7\3\u00f0\n\3\f\3\16\3\u00f3\13\3\3\3\3\3\3\3\3\3\3\3\5\3\u00fa\n\3\3"+
		"\3\5\3\u00fd\n\3\3\3\7\3\u0100\n\3\f\3\16\3\u0103\13\3\3\3\3\3\3\3\3\3"+
		"\3\3\5\3\u010a\n\3\3\3\5\3\u010d\n\3\3\3\7\3\u0110\n\3\f\3\16\3\u0113"+
		"\13\3\3\3\3\3\3\3\7\3\u0118\n\3\f\3\16\3\u011b\13\3\3\3\5\3\u011e\n\3"+
		"\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\5\b\u012a\n\b\3\b\7\b\u012d\n"+
		"\b\f\b\16\b\u0130\13\b\3\t\3\t\3\n\3\n\3\13\3\13\3\f\3\f\3\r\3\r\3\r\3"+
		"\16\3\16\3\16\3\16\3\17\3\17\3\20\3\20\3\21\3\21\5\21\u0147\n\21\3\22"+
		"\3\22\3\22\5\22\u014c\n\22\3\23\3\23\3\23\3\23\3\23\3\23\3\23\6\23\u0155"+
		"\n\23\r\23\16\23\u0156\3\24\3\24\3\24\3\24\5\24\u015d\n\24\3\25\3\25\3"+
		"\26\3\26\5\26\u0163\n\26\3\27\3\27\3\27\3\30\3\30\3\31\3\31\3\32\3\32"+
		"\3\32\3\33\3\33\3\34\3\34\3\34\7\34\u0174\n\34\f\34\16\34\u0177\13\34"+
		"\3\35\3\35\3\36\3\36\5\36\u017d\n\36\3\37\3\37\3\37\2\2 \2\4\6\b\n\f\16"+
		"\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<\2\7\3\2\4\4\3\2\30\32"+
		"\3\2!$\3\2\36\37\3\2%&\2\u01ab\2C\3\2\2\2\4\u011d\3\2\2\2\6\u011f\3\2"+
		"\2\2\b\u0121\3\2\2\2\n\u0123\3\2\2\2\f\u0125\3\2\2\2\16\u0127\3\2\2\2"+
		"\20\u0131\3\2\2\2\22\u0133\3\2\2\2\24\u0135\3\2\2\2\26\u0137\3\2\2\2\30"+
		"\u0139\3\2\2\2\32\u013c\3\2\2\2\34\u0140\3\2\2\2\36\u0142\3\2\2\2 \u0146"+
		"\3\2\2\2\"\u014b\3\2\2\2$\u0154\3\2\2\2&\u015c\3\2\2\2(\u015e\3\2\2\2"+
		"*\u0162\3\2\2\2,\u0164\3\2\2\2.\u0167\3\2\2\2\60\u0169\3\2\2\2\62\u016b"+
		"\3\2\2\2\64\u016e\3\2\2\2\66\u0170\3\2\2\28\u0178\3\2\2\2:\u017c\3\2\2"+
		"\2<\u017e\3\2\2\2>?\5\4\3\2?@\5\34\17\2@B\3\2\2\2A>\3\2\2\2BE\3\2\2\2"+
		"CA\3\2\2\2CD\3\2\2\2D\3\3\2\2\2EC\3\2\2\2FG\7\17\2\2GH\5\36\20\2HI\7\3"+
		"\2\2IK\5 \21\2JL\5\f\7\2KJ\3\2\2\2KL\3\2\2\2LN\3\2\2\2MO\5\32\16\2NM\3"+
		"\2\2\2NO\3\2\2\2OS\3\2\2\2PR\5\30\r\2QP\3\2\2\2RU\3\2\2\2SQ\3\2\2\2ST"+
		"\3\2\2\2TV\3\2\2\2US\3\2\2\2VW\5\6\4\2WX\5.\30\2X\u011e\3\2\2\2Y[\7\20"+
		"\2\2Z\\\5\n\6\2[Z\3\2\2\2[\\\3\2\2\2\\]\3\2\2\2]^\5.\30\2^_\5\6\4\2_a"+
		"\5\36\20\2`b\7\3\2\2a`\3\2\2\2ab\3\2\2\2bd\3\2\2\2ce\5 \21\2dc\3\2\2\2"+
		"de\3\2\2\2eg\3\2\2\2fh\5\f\7\2gf\3\2\2\2gh\3\2\2\2hj\3\2\2\2ik\5\32\16"+
		"\2ji\3\2\2\2jk\3\2\2\2ko\3\2\2\2ln\5\30\r\2ml\3\2\2\2nq\3\2\2\2om\3\2"+
		"\2\2op\3\2\2\2p|\3\2\2\2qo\3\2\2\2rt\7 \2\2su\5\64\33\2ts\3\2\2\2tu\3"+
		"\2\2\2uy\3\2\2\2vx\5\62\32\2wv\3\2\2\2x{\3\2\2\2yw\3\2\2\2yz\3\2\2\2z"+
		"}\3\2\2\2{y\3\2\2\2|r\3\2\2\2|}\3\2\2\2}\u011e\3\2\2\2~\u0082\7\21\2\2"+
		"\177\u0081\n\2\2\2\u0080\177\3\2\2\2\u0081\u0084\3\2\2\2\u0082\u0080\3"+
		"\2\2\2\u0082\u0083\3\2\2\2\u0083\u0085\3\2\2\2\u0084\u0082\3\2\2\2\u0085"+
		"\u0086\5\6\4\2\u0086\u0087\5.\30\2\u0087\u011e\3\2\2\2\u0088\u008c\7\22"+
		"\2\2\u0089\u008b\n\2\2\2\u008a\u0089\3\2\2\2\u008b\u008e\3\2\2\2\u008c"+
		"\u008a\3\2\2\2\u008c\u008d\3\2\2\2\u008d\u011e\3\2\2\2\u008e\u008c\3\2"+
		"\2\2\u008f\u0093\7\23\2\2\u0090\u0092\n\2\2\2\u0091\u0090\3\2\2\2\u0092"+
		"\u0095\3\2\2\2\u0093\u0091\3\2\2\2\u0093\u0094\3\2\2\2\u0094\u011e\3\2"+
		"\2\2\u0095\u0093\3\2\2\2\u0096\u009a\7\24\2\2\u0097\u0099\n\2\2\2\u0098"+
		"\u0097\3\2\2\2\u0099\u009c\3\2\2\2\u009a\u0098\3\2\2\2\u009a\u009b\3\2"+
		"\2\2\u009b\u011e\3\2\2\2\u009c\u009a\3\2\2\2\u009d\u00a1\7\25\2\2\u009e"+
		"\u00a0\n\2\2\2\u009f\u009e\3\2\2\2\u00a0\u00a3\3\2\2\2\u00a1\u009f\3\2"+
		"\2\2\u00a1\u00a2\3\2\2\2\u00a2\u011e\3\2\2\2\u00a3\u00a1\3\2\2\2\u00a4"+
		"\u00a5\7\26\2\2\u00a5\u00a6\5(\25\2\u00a6\u00a7\7\5\2\2\u00a7\u00a9\5"+
		"&\24\2\u00a8\u00aa\5\f\7\2\u00a9\u00a8\3\2\2\2\u00a9\u00aa\3\2\2\2\u00aa"+
		"\u00ac\3\2\2\2\u00ab\u00ad\5\32\16\2\u00ac\u00ab\3\2\2\2\u00ac\u00ad\3"+
		"\2\2\2\u00ad\u00b1\3\2\2\2\u00ae\u00b0\5\30\r\2\u00af\u00ae\3\2\2\2\u00b0"+
		"\u00b3\3\2\2\2\u00b1\u00af\3\2\2\2\u00b1\u00b2\3\2\2\2\u00b2\u011e\3\2"+
		"\2\2\u00b3\u00b1\3\2\2\2\u00b4\u00b5\7\27\2\2\u00b5\u00b7\5\36\20\2\u00b6"+
		"\u00b8\5\f\7\2\u00b7\u00b6\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00ba\3\2"+
		"\2\2\u00b9\u00bb\5\32\16\2\u00ba\u00b9\3\2\2\2\u00ba\u00bb\3\2\2\2\u00bb"+
		"\u00bf\3\2\2\2\u00bc\u00be\5\30\r\2\u00bd\u00bc\3\2\2\2\u00be\u00c1\3"+
		"\2\2\2\u00bf\u00bd\3\2\2\2\u00bf\u00c0\3\2\2\2\u00c0\u00c5\3\2\2\2\u00c1"+
		"\u00bf\3\2\2\2\u00c2\u00c3\5\6\4\2\u00c3\u00c4\5*\26\2\u00c4\u00c6\3\2"+
		"\2\2\u00c5\u00c2\3\2\2\2\u00c5\u00c6\3\2\2\2\u00c6\u011e\3\2\2\2\u00c7"+
		"\u00c8\t\3\2\2\u00c8\u00cb\5.\30\2\u00c9\u00cc\5\6\4\2\u00ca\u00cc\5\b"+
		"\5\2\u00cb\u00c9\3\2\2\2\u00cb\u00ca\3\2\2\2\u00cc\u00cd\3\2\2\2\u00cd"+
		"\u00ce\5\36\20\2\u00ce\u00cf\7\3\2\2\u00cf\u00d1\5 \21\2\u00d0\u00d2\5"+
		"\f\7\2\u00d1\u00d0\3\2\2\2\u00d1\u00d2\3\2\2\2\u00d2\u00d4\3\2\2\2\u00d3"+
		"\u00d5\5\32\16\2\u00d4\u00d3\3\2\2\2\u00d4\u00d5\3\2\2\2\u00d5\u00d9\3"+
		"\2\2\2\u00d6\u00d8\5\30\r\2\u00d7\u00d6\3\2\2\2\u00d8\u00db\3\2\2\2\u00d9"+
		"\u00d7\3\2\2\2\u00d9\u00da\3\2\2\2\u00da\u00df\3\2\2\2\u00db\u00d9\3\2"+
		"\2\2\u00dc\u00de\5,\27\2\u00dd\u00dc\3\2\2\2\u00de\u00e1\3\2\2\2\u00df"+
		"\u00dd\3\2\2\2\u00df\u00e0\3\2\2\2\u00e0\u011e\3\2\2\2\u00e1\u00df\3\2"+
		"\2\2\u00e2\u00e3\7\33\2\2\u00e3\u00e4\5\36\20\2\u00e4\u00e5\7\3\2\2\u00e5"+
		"\u00e6\5 \21\2\u00e6\u00e7\5\6\4\2\u00e7\u00e9\5\60\31\2\u00e8\u00ea\5"+
		"\f\7\2\u00e9\u00e8\3\2\2\2\u00e9\u00ea\3\2\2\2\u00ea\u00ec\3\2\2\2\u00eb"+
		"\u00ed\5\32\16\2\u00ec\u00eb\3\2\2\2\u00ec\u00ed\3\2\2\2\u00ed\u00f1\3"+
		"\2\2\2\u00ee\u00f0\5\30\r\2\u00ef\u00ee\3\2\2\2\u00f0\u00f3\3\2\2\2\u00f1"+
		"\u00ef\3\2\2\2\u00f1\u00f2\3\2\2\2\u00f2\u011e\3\2\2\2\u00f3\u00f1\3\2"+
		"\2\2\u00f4\u00f5\7\34\2\2\u00f5\u00f6\5\36\20\2\u00f6\u00f7\7\3\2\2\u00f7"+
		"\u00f9\5 \21\2\u00f8\u00fa\5\f\7\2\u00f9\u00f8\3\2\2\2\u00f9\u00fa\3\2"+
		"\2\2\u00fa\u00fc\3\2\2\2\u00fb\u00fd\5\32\16\2\u00fc\u00fb\3\2\2\2\u00fc"+
		"\u00fd\3\2\2\2\u00fd\u0101\3\2\2\2\u00fe\u0100\5\30\r\2\u00ff\u00fe\3"+
		"\2\2\2\u0100\u0103\3\2\2\2\u0101\u00ff\3\2\2\2\u0101\u0102\3\2\2\2\u0102"+
		"\u011e\3\2\2\2\u0103\u0101\3\2\2\2\u0104\u0105\7\35\2\2\u0105\u0106\5"+
		"\36\20\2\u0106\u0107\7\3\2\2\u0107\u0109\5 \21\2\u0108\u010a\5\f\7\2\u0109"+
		"\u0108\3\2\2\2\u0109\u010a\3\2\2\2\u010a\u010c\3\2\2\2\u010b\u010d\5\32"+
		"\16\2\u010c\u010b\3\2\2\2\u010c\u010d\3\2\2\2\u010d\u0111\3\2\2\2\u010e"+
		"\u0110\5\30\r\2\u010f\u010e\3\2\2\2\u0110\u0113\3\2\2\2\u0111\u010f\3"+
		"\2\2\2\u0111\u0112\3\2\2\2\u0112\u011e\3\2\2\2\u0113\u0111\3\2\2\2\u0114"+
		"\u0119\7)\2\2\u0115\u0118\5\"\22\2\u0116\u0118\5$\23\2\u0117\u0115\3\2"+
		"\2\2\u0117\u0116\3\2\2\2\u0118\u011b\3\2\2\2\u0119\u0117\3\2\2\2\u0119"+
		"\u011a\3\2\2\2\u011a\u011e\3\2\2\2\u011b\u0119\3\2\2\2\u011c\u011e\7+"+
		"\2\2\u011dF\3\2\2\2\u011dY\3\2\2\2\u011d~\3\2\2\2\u011d\u0088\3\2\2\2"+
		"\u011d\u008f\3\2\2\2\u011d\u0096\3\2\2\2\u011d\u009d\3\2\2\2\u011d\u00a4"+
		"\3\2\2\2\u011d\u00b4\3\2\2\2\u011d\u00c7\3\2\2\2\u011d\u00e2\3\2\2\2\u011d"+
		"\u00f4\3\2\2\2\u011d\u0104\3\2\2\2\u011d\u0114\3\2\2\2\u011d\u011c\3\2"+
		"\2\2\u011e\5\3\2\2\2\u011f\u0120\7\r\2\2\u0120\7\3\2\2\2\u0121\u0122\7"+
		"\16\2\2\u0122\t\3\2\2\2\u0123\u0124\t\4\2\2\u0124\13\3\2\2\2\u0125\u0126"+
		"\t\5\2\2\u0126\r\3\2\2\2\u0127\u0129\5\f\7\2\u0128\u012a\5\32\16\2\u0129"+
		"\u0128\3\2\2\2\u0129\u012a\3\2\2\2\u012a\u012e\3\2\2\2\u012b\u012d\5\30"+
		"\r\2\u012c\u012b\3\2\2\2\u012d\u0130\3\2\2\2\u012e\u012c\3\2\2\2\u012e"+
		"\u012f\3\2\2\2\u012f\17\3\2\2\2\u0130\u012e\3\2\2\2\u0131\u0132\7!\2\2"+
		"\u0132\21\3\2\2\2\u0133\u0134\7\"\2\2\u0134\23\3\2\2\2\u0135\u0136\7#"+
		"\2\2\u0136\25\3\2\2\2\u0137\u0138\7$\2\2\u0138\27\3\2\2\2\u0139\u013a"+
		"\7\6\2\2\u013a\u013b\5\32\16\2\u013b\31\3\2\2\2\u013c\u013d\5\66\34\2"+
		"\u013d\u013e\7\5\2\2\u013e\u013f\t\6\2\2\u013f\33\3\2\2\2\u0140\u0141"+
		"\7\4\2\2\u0141\35\3\2\2\2\u0142\u0143\58\35\2\u0143\37\3\2\2\2\u0144\u0147"+
		"\5<\37\2\u0145\u0147\58\35\2\u0146\u0144\3\2\2\2\u0146\u0145\3\2\2\2\u0147"+
		"!\3\2\2\2\u0148\u014c\5<\37\2\u0149\u014c\7%\2\2\u014a\u014c\7&\2\2\u014b"+
		"\u0148\3\2\2\2\u014b\u0149\3\2\2\2\u014b\u014a\3\2\2\2\u014c#\3\2\2\2"+
		"\u014d\u0155\58\35\2\u014e\u0155\7\7\2\2\u014f\u0155\7\b\2\2\u0150\u0155"+
		"\7\t\2\2\u0151\u0155\7\n\2\2\u0152\u0155\7\3\2\2\u0153\u0155\7\13\2\2"+
		"\u0154\u014d\3\2\2\2\u0154\u014e\3\2\2\2\u0154\u014f\3\2\2\2\u0154\u0150"+
		"\3\2\2\2\u0154\u0151\3\2\2\2\u0154\u0152\3\2\2\2\u0154\u0153\3\2\2\2\u0155"+
		"\u0156\3\2\2\2\u0156\u0154\3\2\2\2\u0156\u0157\3\2\2\2\u0157%\3\2\2\2"+
		"\u0158\u015d\5\66\34\2\u0159\u015d\5<\37\2\u015a\u015d\7%\2\2\u015b\u015d"+
		"\7&\2\2\u015c\u0158\3\2\2\2\u015c\u0159\3\2\2\2\u015c\u015a\3\2\2\2\u015c"+
		"\u015b\3\2\2\2\u015d\'\3\2\2\2\u015e\u015f\5\66\34\2\u015f)\3\2\2\2\u0160"+
		"\u0163\5\66\34\2\u0161\u0163\58\35\2\u0162\u0160\3\2\2\2\u0162\u0161\3"+
		"\2\2\2\u0163+\3\2\2\2\u0164\u0165\7\r\2\2\u0165\u0166\5.\30\2\u0166-\3"+
		"\2\2\2\u0167\u0168\58\35\2\u0168/\3\2\2\2\u0169\u016a\58\35\2\u016a\61"+
		"\3\2\2\2\u016b\u016c\7\f\2\2\u016c\u016d\5\64\33\2\u016d\63\3\2\2\2\u016e"+
		"\u016f\58\35\2\u016f\65\3\2\2\2\u0170\u0175\58\35\2\u0171\u0172\7\3\2"+
		"\2\u0172\u0174\58\35\2\u0173\u0171\3\2\2\2\u0174\u0177\3\2\2\2\u0175\u0173"+
		"\3\2\2\2\u0175\u0176\3\2\2\2\u0176\67\3\2\2\2\u0177\u0175\3\2\2\2\u0178"+
		"\u0179\5:\36\2\u01799\3\2\2\2\u017a\u017d\7\'\2\2\u017b\u017d\5<\37\2"+
		"\u017c\u017a\3\2\2\2\u017c\u017b\3\2\2\2\u017d;\3\2\2\2\u017e\u017f\7"+
		"(\2\2\u017f=\3\2\2\2\66CKNS[adgjoty|\u0082\u008c\u0093\u009a\u00a1\u00a9"+
		"\u00ac\u00b1\u00b7\u00ba\u00bf\u00c5\u00cb\u00d1\u00d4\u00d9\u00df\u00e9"+
		"\u00ec\u00f1\u00f9\u00fc\u0101\u0109\u010c\u0111\u0117\u0119\u011d\u0129"+
		"\u012e\u0146\u014b\u0154\u0156\u015c\u0162\u0175\u017c";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}