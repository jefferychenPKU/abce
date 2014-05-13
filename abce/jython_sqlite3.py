try:
    import sqlite3
    from sqlite3 import * #pylint: disable=W0614


    class Connect:
        def __init__(self, directory, db_name):
            self.db = sqlite3.connect(directory + '/' + db_name + '.db')
            self.cursor = self.db.cursor()

        def execute(self, command):
            self.cursor.execute(command)

        def executeQuery(self, command):
            return self.cursor.execute(command)

        def column_names(self, table_name):
            self.cursor.execute("""PRAGMA table_info(""" + table_name + """)""")
            return [row[1] for row in self.cursor]

        def commit(self):
            self.db.commit()

        def close(self):
            self.db.close()

    class SQLException(Exception):
        pass



except ImportError:
    import java.sql.SQLException as SQLException #pylint: disable=F0401
    import org.sqlite.SQLiteDataSource as SQLiteDataSource #pylint: disable=F0401


    class Connect:
        def __init__(self, directory, db_name):
            dataSource = SQLiteDataSource()
            dataSource.setUrl("jdbc:sqlite:" + directory + '/' + db_name + '.db')
            self.connection = dataSource.getConnection()
            self.cursor = self.connection.createStatement()

        def execute(self, command):
            self.cursor.execute(command)

        def executeQuery(self, command):
            return self.cursor.executeQuery(command)

        def column_names(self, table_name):
            table_info = self.cursor.executeQuery("""PRAGMA table_info(""" + table_name + """)""")
            columns = []
            while True:
                columns.append(table_info.getString(2))
                if not(table_info.next()):
                    break
            return columns

        def commit(self):
            try:
                self.connection.commit()
            except SQLException:
                if self.connection.getAutoCommit() == False:
                    raise

        def close(self):
            self.connection.close()

    class OperationalError(Exception):
        pass

    class InterfaceError(Exception):
        pass
