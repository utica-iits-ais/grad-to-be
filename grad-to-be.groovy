import groovy.sql.Sql

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.sql.ResultSet
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

@GrabConfig(systemClassLoader = true)
@Grab(group='org.duckdb', module='duckdb_jdbc', version='1.2.2.0')
@Grab(group='com.oracle.database.jdbc', module='ojdbc8', version='19.3.0.0')

def inFile = Paths.get(System.properties.'user.dir','files','in','grad-to-be-06-04-2025.xlsx')
def outFilePath = Paths.get(System.properties.'user.dir','files','out','grad-to-be')
def termCode = '202510'

Sql.withInstance('jdbc:duckdb:') { duck ->
    // Create duckdb table from excel sheet
    def crt = duck.execute(
            """
            CREATE TABLE GRAD_SURVEY AS
            SELECT * 
              FROM READ_XLSX(${inFile.toAbsolutePath().toString()},EMPTY_AS_VARCHAR=TRUE)
            """
    )
    def alt = duck.execute(
            """
                ALTER TABLE GRAD_SURVEY ADD COLUMN levl TEXT;
                ALTER TABLE GRAD_SURVEY ADD COLUMN camp TEXT
                """
    )
    def ids = duck.rows(
            "SELECT \"Banner ID\" ID FROM GRAD_SURVEY"
    )

    // Connect to Banner, for each id pull additional data and write to columns on duckdb table
    Sql.withInstance(initBannerProps()) { banner ->
        ids.each {
            additionalBannerData(banner,termCode,it.'ID') { ResultSet rs ->
                if (rs.next()) {
                    updateGradSurvey(duck,it.'ID',rs.getString("LEVL_CODE"),rs.getString("CAMP_CODE"))
                }
            }
        }
    }

    // Using DuckDb features write GRAD_SURVEY table back to execl in partitioned folder structure
    duck.execute(
            """
                COPY (
                SELECT *
                  FROM GRAD_SURVEY 
                ) TO "${outFilePath.toAbsolutePath().toString()}"
                (HEADER TRUE
                ,FORMAT XLSX
                ,PARTITION_BY (LEVL)
                ,OVERWRITE
                ,WRITE_PARTITION_COLUMNS
                ,FILENAME_PATTERN 'grad-to-be-{i}')
                """.toString()
    )


    zipFile(outFilePath)
}

/**
 * Returns result set through closure of additional banner data like level and campus, limited to the IDs loaded into the temp table
 * @param banner
 * @param termCode
 * @param c
 */
def additionalBannerData(Sql banner, String termCode, String bannerId, Closure c) {
    def stmt =
            """
            select i.spriden_id banner_id
                  ,s.sgbstdn_levl_code levl_code
                  ,s.sgbstdn_camp_code camp_code
              from spriden i
                join sgbstdn s
                  on i.spriden_pidm = s.sgbstdn_pidm
             where i.spriden_change_ind is null
               and i.spriden_id = lpad(:bannerId,9,'0')
               and s.sgbstdn_term_code_eff =
                   (select max(s2.sgbstdn_term_code_eff) from sgbstdn s2
                     where s2.sgbstdn_pidm = s.sgbstdn_pidm
                       and s2.sgbstdn_term_code_eff <= :termCode)        
            """
    banner.query(stmt,termCode:termCode,bannerId:bannerId,c)
}

/**
 * Update GRAD_SURVEY record in DuckDB with additional banner data
 * @param duck
 * @param bannerId
 * @param levl
 * @param camp
 * @return
 */
def updateGradSurvey(Sql duck, String bannerId, String levl, String camp) {
    def stmt =
            """
            UPDATE GRAD_SURVEY
               SET LEVL = :levl
                  ,CAMP = :camp
             WHERE "Banner ID" = :id     
            """
    duck.execute(stmt, id:bannerId, levl:levl, camp:camp)
}

/**
 * Take folder containing partitioned xlsx files and turn them in a zip file to be sent
 * @param path
 * @return
 */
def zipFile(Path path) {
    Files.newOutputStream(path.getParent().resolve(path.getFileName().toString() + ".zip"), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING).withCloseable { os ->
        new ZipOutputStream(os).withCloseable { zos ->
            Files.walk(path).forEach { p ->
                def rp = path.relativize(p)
                if (Files.isDirectory(p)) {
                    zos.putNextEntry(new ZipEntry(rp.toString() + '/'))
                    zos.closeEntry()
                } else {
                    zos.putNextEntry(new ZipEntry(rp.toString()))
                    Files.copy(p,zos)
                    zos.closeEntry()
                }
            }
        }
    }
}

/**
 * Banner database connection props
 * @return
 */
def initBannerProps() {
    Properties properties = new Properties()
    Paths.get(System.properties.'user.home','.properties','banner-prod.properties').withInputStream {
        properties.load(it)
    }
    return properties
}
