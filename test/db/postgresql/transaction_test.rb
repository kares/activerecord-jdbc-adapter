require 'db/postgres'
require 'transaction'

class PostgresTransactionTest < Test::Unit::TestCase
  include TransactionTestMethods

  def test_supports_savepoints
    assert_true ActiveRecord::Base.connection.supports_savepoints?
  end

  test '(native) pg-like transaction_status' do
    connection = ActiveRecord::Base.connection.raw_connection
    assert_equal 0, connection.transaction_status

    begin
      Entry.transaction(:isolation => :read_committed) do
        assert_equal 2, connection.transaction_status

        Entry.connection.begin_transaction
        Entry.connection.rollback_transaction

        assert_equal 2, connection.transaction_status
      end
    #rescue
      #assert_equal 0, connection.transaction_status
    end

    assert_equal 0, connection.transaction_status
  end if Test::Unit::TestCase.ar_version('4.0')

end
