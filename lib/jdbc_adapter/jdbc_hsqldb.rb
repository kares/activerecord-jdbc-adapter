module ::JdbcSpec
  module ActiveRecordExtensions
    def hsqldb_connection(config)
      config[:url] ||= "jdbc:hsqldb:#{config[:database]}"
      config[:driver] ||= "org.hsqldb.jdbcDriver"
      embedded_driver(config)
    end

    def h2_connection(config)
      config[:url] ||= "jdbc:h2:#{config[:database]}"
      config[:driver] ||= "org.h2.Driver"
      embedded_driver(config)
    end
  end

  module HSQLDB
    def self.column_selector
      [/hsqldb|\.h2\./i, lambda {|cfg,col| col.extend(::JdbcSpec::HSQLDB::Column)}]
    end

    def self.adapter_selector
      [/hsqldb|\.h2\./i, lambda do |cfg,adapt|
         adapt.extend(::JdbcSpec::HSQLDB)
         def adapt.h2_adapter; true; end if cfg[:driver] =~ /\.h2\./
       end]
    end

    module Column
      private
      def simplified_type(field_type)
        case field_type
        when /longvarchar/i
          :text
        when /tinyint/i
          :boolean
        else
          super(field_type)
        end
      end

      # Override of ActiveRecord::ConnectionAdapters::Column
      def extract_limit(sql_type)
        # HSQLDB appears to return "LONGVARCHAR(0)" for :text columns, which
        # for AR purposes should be interpreted as "no limit"
        return nil if sql_type =~ /\(0\)/
        super
      end

      # Post process default value from JDBC into a Rails-friendly format (columns{-internal})
      def default_value(value)
        # jdbc returns column default strings with actual single quotes around the value.
        return $1 if value =~ /^'(.*)'$/

        value
      end
    end

    def adapter_name #:nodoc:
      defined?(::Jdbc::H2) ? 'h2' : 'hsqldb'
    end

    def modify_types(tp)
      tp[:primary_key] = "INTEGER GENERATED BY DEFAULT AS IDENTITY(START WITH 0) PRIMARY KEY"
      tp[:integer][:limit] = nil
      tp[:boolean][:limit] = nil
      # set text and float limits so we don't see odd scales tacked on
      # in migrations
      tp[:boolean] = { :name => "tinyint" }
      tp[:text][:limit] = nil
      tp[:float][:limit] = 17 if defined?(::Jdbc::H2)
      tp[:string][:limit] = 255
      tp[:datetime] = { :name => "DATETIME" }
      tp[:timestamp] = { :name => "DATETIME" }
      tp[:time] = { :name => "TIME" }
      tp[:date] = { :name => "DATE" }
      tp
    end

    def quote(value, column = nil) # :nodoc:
      return value.quoted_id if value.respond_to?(:quoted_id)

      case value
      when String
        if respond_to?(:h2_adapter) && value.empty?
          "''"
        elsif column && column.type == :binary
          "'#{value.unpack("H*")}'"
        else
          "'#{quote_string(value)}'"
        end
      else super
      end
    end

    def quote_column_name(name) #:nodoc:
      name = name.to_s
      if name =~ /[-]/
        %Q{"#{name.upcase}"}
      else
        name
      end
    end

    def quote_string(str)
      str.gsub(/'/, "''")
    end

    def quoted_true
      '1'
    end

    def quoted_false
      '0'
    end

    def add_column(table_name, column_name, type, options = {})
      if option_not_null = options[:null] == false
        option_not_null = options.delete(:null)
      end
      add_column_sql = "ALTER TABLE #{quote_table_name(table_name)} ADD #{quote_column_name(column_name)} #{type_to_sql(type, options[:limit], options[:precision], options[:scale])}"
      add_column_options!(add_column_sql, options)
      execute(add_column_sql)
      if option_not_null
        alter_column_sql = "ALTER TABLE #{quote_table_name(table_name)} ALTER #{quote_column_name(column_name)} NOT NULL"
      end
    end

    def change_column(table_name, column_name, type, options = {}) #:nodoc:
      execute "ALTER TABLE #{table_name} ALTER COLUMN #{column_name} #{type_to_sql(type, options[:limit])}"
    end

    def change_column_default(table_name, column_name, default) #:nodoc:
      execute "ALTER TABLE #{table_name} ALTER COLUMN #{column_name} SET DEFAULT #{quote(default)}"
    end

    def rename_column(table_name, column_name, new_column_name) #:nodoc:
      execute "ALTER TABLE #{table_name} ALTER COLUMN #{column_name} RENAME TO #{new_column_name}"
    end

    def rename_table(name, new_name)
      execute "ALTER TABLE #{name} RENAME TO #{new_name}"
    end

    def insert(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil) #:nodoc:
      log(sql,name) do
        @connection.execute_update(sql)
      end
      table = sql.split(" ", 4)[2]
      id_value || last_insert_id(table, nil)
    end

    def last_insert_id(table, sequence_name)
      Integer(select_value("CALL IDENTITY()"))
    end

    # Override normal #_execute: See Rubyforge #11567
    def _execute(sql, name = nil)
      if ::ActiveRecord::ConnectionAdapters::JdbcConnection::select?(sql)
        @connection.execute_query(sql)
      elsif ::ActiveRecord::ConnectionAdapters::JdbcConnection::insert?(sql)
        insert(sql, name)
      else
        @connection.execute_update(sql)
      end
    end

    def add_limit_offset!(sql, options) #:nodoc:
      offset = options[:offset] || 0
      bef = sql[7..-1]
      if limit = options[:limit]
        sql.replace "select limit #{offset} #{limit} #{bef}"
      elsif offset > 0
        sql.replace "select limit #{offset} 0 #{bef}"
      end
    end

    # override to filter out system tables that otherwise end
    # up in db/schema.rb during migrations.  JdbcConnection#tables
    # now takes an optional block filter so we can screen out
    # rows corresponding to system tables.  HSQLDB names its
    # system tables SYSTEM.*, but H2 seems to name them without
    # any kind of convention
    def tables
      @connection.tables.select {|row| row.to_s !~ /^system_/i }
    end

    def remove_index(table_name, options = {})
      execute "DROP INDEX #{quote_column_name(index_name(table_name, options))}"
    end
  end
end
