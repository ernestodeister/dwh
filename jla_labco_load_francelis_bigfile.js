jla_labco_load_francelis_bigfile("FR-FRL-01");

function jla_labco_load_francelis_bigfile(p_source) {
    // -------------------------------------------------------------------------
    // local_log
    // -------------------------------------------------------------------------
    function class_processLog(p_con_code, p_dirname) {
        this.m_log2file = false;
        this.m_con_code = p_con_code;
        this.m_dirname = p_dirname;

        this.log = function(p_msg) {
            if (this.m_log2file) {
                console.log(`${new Ax.util.Date().format("HH:mm:ss")} : ${this.m_con_code}[${this.m_dirname}] - ${p_msg}`)
            }
            Ax.db.update("labco_ftpconnect", {
                "con_errmsg": this.m_dirname + ": " + p_msg
            }, {
                "con_code": this.m_cod_code
            });
        };

        this.getConCode = function() {
            return this.m_con_code;
        }
    }
    
    function splitintoChunks(filePath) {
        let file = new Ax.io.File(filePath);
        const BufferedReader = Java.type("java.io.BufferedReader");
        const BufferedWriter = Java.type("java.io.BufferedWriter");
        const MAP_SIZE = 2*1024*1024*1024; // 2GB
        if (file.length() >= MAP_SIZE) 
        {
    
            const basePath  = file.getParentFile();
            const baseName  = file.getName();
            let nchunks     = Math.ceil(file.length() / MAP_SIZE);
    
            let files       = Array.apply(null,{length: nchunks}).map((_, i) =>  new Ax.io.File(`${basePath}/chunk_${i}_${baseName}`)); 
    
            let writers     = files.map((file) =>  new BufferedWriter(file.toWriter()) );
    
            const reader = new BufferedReader(file.toReader());
    
            let line;  
            let counter = 0;
            let bytes_read = 0;
            while((line=reader.readLine())!=null)  
            {  
    
                let fileIndex = Math.floor(bytes_read / MAP_SIZE)
                writers[fileIndex].write(line +"\n");
    
                counter++;
    
                bytes_read += line.getBytes().length;   
                // if (counter % 10000 == 0) {
                //     console.log("READ ROWS " +  counter +"  total size " + bytes_read);
                // }
    
            }  
            writers.forEach(f => f.flush());
            return files;
    
        } else {
            return [file];    
        }
    }

    function local_create_temp_tables(){
        Ax.db.execute(`DROP TABLE IF EXISTS @load_francelis`);
        Ax.db.execute(`DROP TABLE IF EXISTS @temp_load_request`);
        Ax.db.execute(`DROP TABLE IF EXISTS @temp_load_testorder`);
        Ax.db.execute(`
            <!--
            "SEL";"CODE_SEL";"BUILDING";"CODE_BUIDING";"CODE_BUILDING_MDM";"N_DOSSIER";"DATE_CREATION";"CIVILITE_PATIENT";"NOM_NAISSANCE_PATIENT";"NOM_USAGE_PATIENT";"PRENOM_PATIENT";"DDN_PATIENT";"NUM_SSV_PATIENT";"SEXE_PATIENT";"CIVILITE_PRESCRIPTEUR";"NOM_PRESCRIPTEUR";"PRENOM_PRESCRIPTEUR";"ID_PRESCRIPTEUR";"ADELI_PRESCRIPTEUR";"ID_RPPS";"NOM_AMO";"CODE_AMO";"CENTRE_AMO";"NOM_AMC";"CODE_AMC";"ID_AMC";"TOTAL_DOSSIER";"TOTAL_AMO";"TOTAL_AMC";"TOTAL_CORRESPONDANT";"TOTAL_PATIENT";"MT_SUPPL_PAT";"CODE_ACTE_BIO";"PRIX_ACTE_BIO";"CODE_ANALYSE"
            "BARLA";"BAR";"CIMIEZ";"CMZ";"FR.06000.116";4731;"2019/01/25 11:09:35";"Mr";"SARFATI";"SARFATI";"Lucien";01/12/35;"1351299351732";"M";"Dr";"ALBAGLY";"MARLENE";9796909;"061127080";"10100396182";"R.A.M (PROF. LIB. DE PROVINCES)";"03054";"1300";"MUTUELLE ISANTE";"1ISANT";"0000733931";19,85;19,85;0;0;0;0;"A1C";0;"A1C1"
            "BARLA";"BAR";"CIMIEZ";"CMZ";"FR.06000.116";4731;"2019/01/25 11:09:35";"Mr";"SARFATI";"SARFATI";"Lucien";01/12/35;"1351299351732";"M";"Dr";"ALBAGLY";"MARLENE";9796909;"061127080";"10100396182";"R.A.M (PROF. LIB. DE PROVINCES)";"03054";"1300";"MUTUELLE ISANTE";"1ISANT";"0000733931";19,85;19,85;0;0;0;0;"C.";0;"C.."
            "BARLA";"BAR";"CIMIEZ";"CMZ";"FR.06000.116";4731;"2019/01/25 11:09:35";"Mr";"SARFATI";"SARFATI";"Lucien";01/12/35;"1351299351732";"M";"Dr";"ALBAGLY";"MARLENE";9796909;"061127080";"10100396182";"R.A.M (PROF. LIB. DE PROVINCES)";"03054";"1300";"MUTUELLE ISANTE";"1ISANT";"0000733931";19,85;19,85;0;0;0;0;"CTBIOC";0;"BIOHEM"
            -->
            <table name='@load_francelis' temp='yes'>
                <column name='req_entity_name'    type='varchar' size='40' />  <!-- SEL (Entity Name) -->
                <column name='req_entity'         type='varchar' size='15' />  <!-- Mormalized local entity (Company) -->
                <column name='req_labcode_name'   type='varchar' size='40' />  <!-- Building -->
                <column name='req_labcode'        type='varchar' size='15' />  <!-- Building code-->
                <column name='req_labcode_mdm'    type='varchar' size='15' />  <!-- Building code MDM-->
                <column name='req_code'           type='varchar' size='32' />  <!-- Unique key (lab+code). If repeatable, send YYYYMM+Req_code -->
                <column name='req_date_created'   type='varchar' size='32' />               <!-- Creation date of request -->
                <column name='pat_civilite'       type='varchar' size='15' />  <!--  -->
                <column name='pat_nom_naissance'  type='varchar' size='64' />  <!--  -->
                <column name='pat_nom_usage'      type='varchar' size='64' />  <!--  -->
                <column name='pat_prenom'         type='varchar' size='64' />  <!--  -->
                <column name='pat_don'            type='varchar' size='64' />  <!--  -->
                <column name='pat_num_ssv'        type='varchar' size='64' />  <!--  -->
                <column name='pat_sex'            type='char' size='1' />  <!--  -->
                <column name='pre_civilite'       type='varchar' size='15' />  <!-- Civilite prescripteur -->
                <column name='pre_nom'            type='varchar' size='64' />  <!--  -->
                <column name='pre_prenom'         type='varchar' size='64' />  <!--  -->
                <column name='pre_code'           type='varchar' size='64' />  <!--  -->
                <column name='pre_adeli'          type='varchar' size='64' />  <!-- Unique ID Prescriptor -->
                <column name='pre_rpps'           type='varchar' size='64' />  <!-- RPPS ID Prescriptor -->
                <column name='amo_nom'            type='varchar' size='64' />  <!--  -->
                <column name='amo_code'           type='varchar' size='64' />  <!--  -->
                <column name='amo_centre'         type='varchar' size='64' />  <!--  -->
                <column name='amc_nom'            type='varchar' size='64' />  <!--  -->
                <column name='amc_code'           type='varchar' size='64' />  <!--  -->
                <column name='amc_id'             type='varchar' size='64' />  <!--  -->

                <column name='req_amount_eur'   type='decimal' size='10,5' /><!-- estimated Net amount on euros -->
                <column name='req_amo_amount'   type='decimal' size='10,5' /><!--  -->
                <column name='req_amc_amount'   type='decimal' size='10,5' /><!--  -->
                <column name='req_corr_amount'  type='decimal' size='10,5' /><!--  -->
                <column name='req_patient_amount' type='decimal' size='10,5' /><!--  -->
                <column name='req_mt_suppl_pat' type='decimal' size='10,5' /><!--  -->
                
                <column name='code_acte_bio'    type='varchar' size='10' /><!--  -->
                <column name='prix_acte_bio'    type='varchar' size='10' /><!--  -->
                <column name='code_analyse'     type='varchar' size='15' /><!--  EYH: previous size=10-->
            </table>
            <!-- <index table='@load_francelis' name='@load_francelis_i' columns='req_entity, req_labcode_mdm, req_code' /> -->
        `);

        Ax.db.execute(`
            CREATE INDEX @load_francelis_i
        ON @load_francelis(req_entity, req_labcode_mdm, req_code);
        `);

        Ax.db.execute(`
            CREATE INDEX @load_francelis_i2
        ON @load_francelis(req_labcode_mdm, req_code, code_analyse);
        `);

    }
    
    Ax.db.execute(`SET ENVIRONMENT use_dwa '0';`);

    console.log(`Searching labco_ftpconnect.con_code  MATCHES '${p_source}'`);
    
    // =========================================================================
    // FOREACH  labco_ftpconnect
    // =========================================================================
    var rs_ftpconnect = Ax.db.executeQuery(`
        SELECT *
          FROM labco_ftpconnect, labco_ftpservers
         WHERE labco_ftpconnect.ftp_code     = labco_ftpservers.ftp_code AND
            labco_ftpconnect.con_status   = 'A' AND
            labco_ftpconnect.con_loadtype = 'F' AND
            labco_ftpconnect.con_loadstat  = 0  AND
            labco_ftpconnect.con_code MATCHES ?
        `, p_source);
        
        for (var labco_ftpconnect of rs_ftpconnect) {
            labco_ftpconnect.con_ftpdir = (labco_ftpconnect.con_ftpdir == null) ? '' : labco_ftpconnect.con_ftpdir;
            console.log(`=======================================================`);
            console.log(`LOADING SERVER ${labco_ftpconnect.con_code}`);
            console.log(`=======================================================`);

            if (labco_ftpconnect.con_loadstat > 0 &&
                Ax.util.Date.hours(new Ax.util.Date(labco_ftpconnect.con_loadstart), new Ax.util.Date()) < 12) {
                console.log(`SERVER ${labco_ftpconnect.con_code} ERROR. LOADING ALREADY RUNNING.`);
                console.log(`===================================================================`);
                // Connection is already loading. Mark error motive on connection
                Ax.db.update("labco_ftpconnect", {
                    "con_errnum": labco_ftpconnect.con_errnum++,
                    "con_errmsg": "Loading already running..."
                }, {
                    "con_code": labco_ftpconnect.con_code
                });
                continue;
            }
    
    
            Ax.db.update("labco_ftpconnect", {
                "con_loadstat": 1,
                "con_loadstart": new Ax.sql.Date(),
                "con_loadend": null
            }, {
                "con_code": labco_ftpconnect.con_code
            });
            if (Ax.db.isOnTransaction()) {
                Ax.db.commitWork();
            }
            Ax.db.beginWork();

            // =========================================================================
            // ACCESS TO SERVER DIRECTORY
            // =========================================================================
            var log_process = {
                log_procnum : null
            };

            // try {
                // =========== TEST ===========
                // labco_ftpconnect.ftp_dirbase = labco_ftpconnect.ftp_dirbase.replace("/home/cards", "/home/axional/cards_test");
                labco_ftpconnect.ftp_dirbase = labco_ftpconnect.ftp_dirbase.replace("/home/cards", "/home/cards");
                // ============================
                var m_con_folder_path = labco_ftpconnect.ftp_dirbase + labco_ftpconnect.con_ftpdir;
                console.log(`Scanning CSV files in:\n ${m_con_folder_path}`);
                
                // =========================================================================
                // Foreach folder that matches pattern (Foreach day with data)
                // =========================================================================
                var folder = new Ax.io.File(m_con_folder_path);
                
                if (!folder.isDirectory()) {
                    throw new Error(`Folder ${m_con_folder_path} is not a reachable directory`)
                }
                 
                // only files starting with YEAR 20... and csv type
                // Sort alphabetically
                var m_ListFileDir = folder.listFiles(f => {
                                                        return f.getName().matches("20.*\\.csv\\.ano");
                                                      })
                m_ListFileDir.sort((a,b) => a.getName().compareTo(b.getName()))
                
                for (var m_FileSetDir of m_ListFileDir) {
                     
                    // if (!m_FileSetDir.isDirectory()) {
                    if (!m_FileSetDir.isFile()) {
                        continue;
                    }
                    
                    // If Folder already loaded goto next folder
                    if (m_FileSetDir.getName().endsWith("LOADED")) {
                        continue;
                    }

                    // 2020-03-28: VSC
                    // Filesets should be writable by user/group to avoid error in renaming
                    if (!m_FileSetDir.canWrite()) {
                        throw new Error(`${m_FileSetDir.getName()} : Doesn't have write permissions for group`)
                    }

                    var m_dirname = m_FileSetDir.getName();
                    var m_fatalerrors = 0;
                    var m_totalnumrows = 0;
                    var m_load_date_start = new Ax.util.Date();
                    var m_date_created = new Ax.sql.Date();
                    var p_fileObject = new Ax.io.File(m_dirname);
                    // var m_loaderr_dossier = 0;
                    // var m_loaderr_doscotation = 0;

                    var m_processlog = new class_processLog(labco_ftpconnect.con_code, m_dirname);

                    m_processlog.m_log2file = true;
                    // Control de un margen de diez minutos por seguridad
                    if (Ax.util.Date.minutes(new Ax.util.Date(m_FileSetDir.getLastModified()), new Ax.util.Date()) < 0) {
                        console.log(`${m_dirname} has less than 10 minutes - skipped from load until becomes older`);
                        continue;
                    }

                    // Protection measure: foreach folder trying to load, remark connection with loading flag
                    Ax.db.update("labco_ftpconnect", {
                        "con_loadstat": 1
                    }, {
                        "con_code": labco_ftpconnect.con_code
                    });

                    var m_date_loaded = Ax.util.Date.parse("yyyyMMdd", m_dirname.substring(0, 8));
                    //var m_date_loaded = new Ax.util.Date(m_dirname.substring(0, 4), m_dirname.substring(4, 6), m_dirname.substring(6, 8));

                    var log_process = {
                        "log_procnum": 0,
                        "date_created": new Ax.sql.Date(),
                        "user_created": Ax.db.getUser(),
                        "procname": "labco_load_francelis",
                        "load_server": labco_ftpconnect.con_code,
                        "load_folder": m_dirname,
                        "load_date": m_date_loaded
                    }
                    Ax.db.delete("log_process", {
                        "load_server": log_process.load_server,
                        "load_folder": m_dirname,
                        "load_date": log_process.load_date
                    });
                    log_process.log_procnum = Ax.db.insert("log_process", log_process).getSerial();
                    if (Ax.db.isOnTransaction()) {
                        Ax.db.commitWork();
                    }
                    Ax.db.beginWork();

                    // Creamos las tablas temporales necesarias para la carga
                    console.log("");
                    m_processlog.log("Create temporal loading tables");
                    var a = local_create_temp_tables();

                    // =========================================================================
                    // MAIN PROCESSING LOOP
                    // =========================================================================
                    m_processlog.log(`Directory ${m_dirname}`);
                    console.log("=============================================================");
                    console.log(`Server.....: ${labco_ftpconnect.con_code}`);
                    console.log(`File..: ${m_dirname}`);
                    console.log("=============================================================");
                    
                    // <set name='m_filerows'><file.getLines><m_ftpDirFile /></file.getLines></set>
                    var m_filerows = m_FileSetDir.getLineCount();
                    //TEST
                    var p_processlog = m_processlog;
                    p_processlog.log(`Counting row(s) from ${p_fileObject.getName()}`);
                    // p_processlog.log(`Loading ${m_filerows} row(s) into ${p_tabname} from ${p_fileObject.getName()}`);
                    var m_full_path = m_con_folder_path +"/"+p_fileObject;
                    
                    // Load FranceListFolder
                    var m_counter_err_reader = 0;
                    var m_counter_ins = null;
                    var m_counter_upd = null;
                    var m_counter_err = null;

                    if(m_filerows > 0){
                        m_counter_ins = 0;
                        m_counter_upd = 0;
                        m_counter_err = 0;

                        splitintoChunks(m_full_path).forEach(
                            function load_francelis_csv(file){
                                var rs_writer = new Ax.rs.Reader().csv(options => {
                                                 
                                    options.setFile(file);
                                    options.setMemoryMapped(true);
                                    options.setDelimiter("\t");
                                    // options.setQuoteChar("¡");
                                    options.setQuoteChar("\"");
                                    options.setCharset("utf-8");
                                    options.setHeader(false);
                                    options.setIncludeColumnIndexes(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34)
                                    options.setColumnNameMapping([
                                        "req_entity_name", "req_entity", "req_labcode_name", "req_labcode", "req_labcode_mdm", 
                                        "req_code", "req_date_created", "pat_civilite", "pat_nom_naissance","pat_nom_usage",
                                        "pat_prenom", "pat_don", "pat_num_ssv", "pat_sex", "pre_civilite", "pre_nom", "pre_prenom",
                                        "pre_code", "pre_adeli", "pre_rpps", "amo_nom", "amo_code", "amo_centre", "amc_nom",
                                        "amc_code", "amc_id", "req_amount_eur", "req_amo_amount", "req_amc_amount", 
                                        "req_corr_amount", "req_patient_amount", "req_mt_suppl_pat", "code_acte_bio",
                                        "prix_acte_bio", "code_analyse"
                                        ]);
                                        
                                    options.setErrorHandler(error => {
                                      var log_events = {
                                        "log_id"      : 0,
                                        "log_procnum" : p_log_process.log_procnum,
                                        "subsystem"   : p_processlog.getConCode(),
                                        "tablename"   : p_tabname,
                                        "event_type"  : "E0010",
                                        "msglog1"     : String(error.getRecord()),
                                        "msglog2"     : String(error.getException()),
                                        "numlog1"     : error.getRow()
                                        //"numlog2"     : error.getColumn(),
                                        //"numlog3"     : error.getSQLCode(),
                                        //"numlog4"     : error.getErrorCode()
                                      };
                                      Ax.db.insert("log_events", log_events);
                                      m_counter_err_reader++;
                                      // Continue ignoring error
                                      return true;
                                    });
                            
                                    // NULL format
                                    options.setColumnFilterMap("req_entity_name", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("req_entity", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("req_labcode_name", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("req_labcode", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("req_labcode_mdm", (src) => {return src == "\\N" ? null : src;});
                            
                                    options.setColumnFilterMap("req_code", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("req_date_created", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("pat_civilite", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("pat_nom_naissance", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("pat_nom_usage", (src) => {return src == "\\N" ? null : src;});
                            
                                    options.setColumnFilterMap("pat_prenom", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("pat_don", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("pat_num_ssv", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("pat_sex", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("pre_civilite", (src) => {return src == "\\N" ? null : src;});
                            
                                    options.setColumnFilterMap("pre_nom", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("pre_prenom", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("pre_code", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("pre_adeli", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("pre_rpps", (src) => {return src == "\\N" ? null : src;});
                            
                                    options.setColumnFilterMap("amo_nom", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("amo_code", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("amo_centre", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("amc_nom", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("amc_code", (src) => {return src == "\\N" ? null : src;});
                            
                                    options.setColumnFilterMap("amc_id", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("req_amount_eur", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("req_amo_amount", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("req_amc_amount", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("req_corr_amount", (src) => {return src == "\\N" ? null : src;});
                            
                                    options.setColumnFilterMap("req_patient_amount", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("req_mt_suppl_pat", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("code_acte_bio", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("prix_acte_bio", (src) => {return src == "\\N" ? null : src;});
                                    options.setColumnFilterMap("code_analyse", (src) => {return src == "\\N" ? null : src;});                          
                            
                                    // Columns type
                                    options.setColumnType("req_entity_name", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("req_entity", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("req_labcode_name", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("req_labcode", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("req_labcode_mdm", Ax.sql.Types.VARCHAR);
                            
                                    options.setColumnType("req_code", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("req_date_created", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("pat_civilite", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("pat_nom_naissance", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("pat_nom_usage", Ax.sql.Types.VARCHAR);
                            
                                    options.setColumnType("pat_prenom", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("pat_don", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("pat_num_ssv", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("pat_sex", Ax.sql.Types.CHAR);
                                    options.setColumnType("pre_civilite", Ax.sql.Types.VARCHAR);
                            
                                    options.setColumnType("pre_nom", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("pre_prenom", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("pre_code", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("pre_adeli", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("pre_rpps", Ax.sql.Types.VARCHAR);
                            
                                    options.setColumnType("amo_nom", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("amo_code", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("amo_centre", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("amc_nom", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("amc_code", Ax.sql.Types.VARCHAR);
                            
                                    options.setColumnType("amc_id", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("req_amount_eur", Ax.sql.Types.FLOAT);
                                    options.setColumnType("req_amo_amount", Ax.sql.Types.FLOAT);
                                    options.setColumnType("req_amc_amount", Ax.sql.Types.FLOAT);
                                    options.setColumnType("req_corr_amount", Ax.sql.Types.FLOAT);
                            
                                    options.setColumnType("req_patient_amount", Ax.sql.Types.FLOAT);
                                    options.setColumnType("req_mt_suppl_pat", Ax.sql.Types.FLOAT);
                                    options.setColumnType("code_acte_bio", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("prix_acte_bio", Ax.sql.Types.VARCHAR);
                                    options.setColumnType("code_analyse", Ax.sql.Types.VARCHAR);                        
                                })
                                .writer();
                                // ;
                                // console.log(rs_writer.rows().first(100));
                                // throw new Error("LLEGA ");
                                rs_writer.db(options => {
                                        // options.setLogger(console.getLogger());
                                        options.setConnection(Ax.db.getObject());
                                        options.setTableName('@load_francelis');
                                        options.setInsertFirst(true);
                                        options.setBatchSize(5000);
                                        options.setErrorHandler(error => {
                                            var log_events = {
                                                "log_id": 0,
                                                "log_procnum": log_process.log_procnum,
                                                "subsystem": p_processlog.getConCode(),
                                                "tablename": "@load_francelis",
                                                "event_type": "E0010",
                                                "msglog1" : String(error.getData()),
                                                "msglog2" : String (error.getMessage()),
                                                "numlog1": error.getRow(),
                                                // "numlog2"     : error.getColumn(),
                                                // "numlog3"     : error.getSQLCode(),
                                                "numlog4": String (error.getErrorCode())
                                            };
                                            Ax.db.insert("log_events", log_events);
                            
                                            // Continue ignoring error
                                            return true;
                                        });
                                    }
                            
                                );
                                m_counter_ins = m_counter_ins + rs_writer.getStatistics().getInsertCount();
                                m_counter_upd = m_counter_upd + rs_writer.getStatistics().getUpdateCount();
                                m_counter_err = m_counter_err + rs_writer.getStatistics().getErrorCount();
                                // m_counter = rs_writer.getStatistics();
                            }
                        );
                    }
                    
                    // throw new Error("LLEGA " + m_counter);
                    if (m_counter_ins != null ) {
                        var m_counter_rows = m_counter_ins + m_counter_upd + (m_counter_err + m_counter_err_reader);
                        var m_log_process_files = {
                            "log_regid": 0,
                            "log_procnum": log_process.log_procnum,
                            "log_tabname": "@load_francelis",
                            "date_created": m_date_created,
                            "date_finished": new Ax.sql.Date(),
                            "rows_file": m_counter_rows,
                            "rows_insert": m_counter_ins,
                            "rows_update": m_counter_upd,
                            "rows_error": (m_counter_err + m_counter_err_reader)
                        };
                        Ax.db.insert("log_process_files", m_log_process_files);

                        var m_rowsloaded = m_counter_rows - (m_counter_err + m_counter_err_reader);
                        m_processlog.log(`Processed ${m_rowsloaded} row(s) of ${m_counter_rows} , ${m_counter_err + m_counter_err_reader} error(s)`);

                        if (Ax.db.isOnTransaction()) {
                            Ax.db.commitWork();
                        }

                        m_totalnumrows = m_totalnumrows + m_counter_rows;
                    } else if (m_counter_ins == null) {
                        
                        var m_counter_rows = 0;
                        var m_log_process_files = {
                            "log_regid": 0,
                            "log_procnum": log_process.log_procnum,
                            "log_tabname": "@load_francelis",
                            "date_created": m_date_created,
                            "date_finished": new Ax.sql.Date(),
                            "rows_file": m_counter_rows,
                            "rows_insert": 0,
                            "rows_update": 0,
                            "rows_error": 0
                        };
                        Ax.db.insert("log_process_files", m_log_process_files);

                        var m_rowsloaded = 0;
                        m_processlog.log(`Processed ${m_rowsloaded} row(s) of ${m_counter_rows} , ${m_rowsloaded} error(s)`);

                        if (Ax.db.isOnTransaction()) {
                            Ax.db.commitWork();
                        }

                        m_totalnumrows = m_totalnumrows + m_counter_rows;
                    }

                    Ax.db.execute(`
                    UPDATE @load_francelis SET pat_num_ssv = 'A-' || ifx_checksum(pat_prenom, ifx_checksum(pat_nom_usage, 0))
                    WHERE pat_num_ssv IS NULL OR pat_num_ssv=''
                    `);

                    var m_requests_quality = 0
                    var m_load_date_end = new Ax.util.Date()

                    var m_load_date_sec = Ax.util.Date.seconds(m_load_date_start, m_load_date_end)
    
                    var m_load_ratio = (m_load_date_sec > 0 ? (m_totalnumrows / m_load_date_sec) : 0);
    
                    Ax.db.execute(`
                      UPDATE labco_ftpconnect
                          SET con_lastcon = CURRENT,
                            con_rowsnum = ${m_totalnumrows},
                            con_rowssec = (NVL(con_rowssec, ${m_load_ratio}) + ${m_load_ratio}) / 2,
                            con_dateload = CASE WHEN ${m_date_loaded.setConnection(Ax.db)} > NVL(con_dateload, MDY(1,1,2000)) THEN ${m_date_loaded.setConnection(Ax.db)} ELSE con_dateload END,
                            con_errnum = 0, 
                            con_errmsg = NULL
                      WHERE
                          con_code = "${labco_ftpconnect.con_code}"
                  `);
    
                    if (Ax.db.isOnTransaction()) {
                        Ax.db.commitWork();
                    }

                    // =================================================================== 
                    // LOAD TABLES                                                         
                    // =================================================================== 

                    m_processlog.log(`Update statistics on temp fact tables`);

                    Ax.db.execute(`UPDATE STATISTICS FOR TABLE @load_francelis`);

                    // ========= VIRTUAL BUILDING ============
                    m_processlog.log(`Loading table: labco_building`);
                    
                    var rs_labco_building = new Ax.rs.Reader().db(options => {
                        // JLA corroborar campos de select
                        options.setResultSet(Ax.db.executeQuery(`
                            SELECT 
                                DISTINCT 
                                -- 0 seqno, 
                                'FR.00' || req_entity[2,4] || '.' || 
                                    CASE WHEN req_labcode = 'COV'  AND substr(req_labcode_mdm, 10) = '000' THEN '001'
                                        WHEN req_labcode = 'UEFA' AND substr(req_labcode_mdm, 10) = '000' THEN '002'
                                        ELSE nvl(substr(req_labcode_mdm, 10), '999')
                                    END build_cd, 
                                req_labcode_name build_name, 
                                req_labcode gpm_codcen,
                                '011' build_type, 
                                'O' build_isasset,
                                '00' || req_entity[2,4] build_zipcode, 
                                'FR' build_country, 
                                'Virtual' build_cityname, 
                                'FRA' build_zonimp, 
                                'fr' build_idioma                                             
                            FROM @load_francelis
                            WHERE 
                                req_labcode_mdm MATCHES 'FR.00*'
                    
                        `));
                        // options.setColumnNameMapping(["servercode","pat_code"]);
                    })
                    .writer();
                    //CONFIRMAR
                    // ;

                    // throw new Error("LLEGA ");
                    var m_date_created_building = new Ax.sql.Date();
                    rs_labco_building.db(options => {
                        // options.setLogger(console.getLogger());
                        options.setConnection(Ax.db.getObject());
                        options.setTablePrimaryKeyColumns("build_cd");
                        options.setTableName("labco_building");
                        options.setBatchSize(5000);
                        // options.setInsertFirst(true);
                        options.setErrorHandler(error => {

                            console.log("log_procnum", log_process.log_procnum);
                            console.log("subsystem",     p_processlog.getConCode());
                            console.log("event_type", "E0010");
                            console.log("tablename", "labco_building");
                            console.log("msglog1", String(error.getData()));
                            console.log("msglog2", String (error.getMessage()));
                            console.log("numlog1", error.getRow());
                            console.log("numlog4", String (error.getErrorCode()));


                            var log_events = {
                                "log_id": 0,
                                "log_procnum": log_process.log_procnum,
                                "subsystem"   : p_processlog.getConCode(),
                                "event_type": "E0010",
                                "tablename": "labco_building",
                                "msglog1" : String(error.getData()),
                                "msglog2" : String (error.getMessage()),
                                "numlog1": error.getRow(),
                                // "numlog2"     : error.getColumn(),
                                // "numlog3"     : error.getSQLCode(),
                                "numlog4": String (error.getErrorCode())
                            };
                            Ax.db.insert("log_events", log_events);
    
                            // Continue ignoring error
                            return true;
                        });
                    });
                    m_counter = rs_labco_building.getStatistics();

                    if (m_counter != null && m_counter != -1) {
                        var m_counter_rows = m_counter.getInsertCount() + m_counter.getUpdateCount() + m_counter.getErrorCount();
                        var m_log_process_files = {
                            "log_regid": 0,
                            "log_procnum": log_process.log_procnum,
                            "log_tabname": "labco_building",
                            "date_created": m_date_created_building,
                            "date_finished": new Ax.sql.Date(),
                            "rows_file": m_counter_rows,
                            "rows_insert": m_counter.getInsertCount(),
                            "rows_update": m_counter.getUpdateCount(),
                            "rows_error": m_counter.getErrorCount()
                        };
                        Ax.db.insert("log_process_files", m_log_process_files);

                        var m_rowsloaded = m_counter_rows - m_counter.getErrorCount();
                        m_processlog.log(`Processed ${m_rowsloaded} row(s) of ${m_counter_rows} , ${m_counter.getErrorCount()} error(s)`);

                        if (Ax.db.isOnTransaction()) {
                            Ax.db.commitWork();
                        }

                        m_totalnumrows = m_totalnumrows + m_counter_rows;
                    } else if (m_counter == -1) {
                        
                        var m_counter_rows = 0;
                        var m_log_process_files = {
                            "log_regid": 0,
                            "log_procnum": log_process.log_procnum,
                            "log_tabname": "labco_building",
                            "date_created": m_date_created_building,
                            "date_finished": new Ax.sql.Date(),
                            "rows_file": m_counter_rows,
                            "rows_insert": 0,
                            "rows_update": 0,
                            "rows_error": 0
                        };
                        Ax.db.insert("log_process_files", m_log_process_files);

                        var m_rowsloaded = 0;
                        m_processlog.log(`Processed ${m_rowsloaded} row(s) of ${m_counter_rows} , ${m_rowsloaded} error(s)`);

                        if (Ax.db.isOnTransaction()) {
                            Ax.db.commitWork();
                        }

                        m_totalnumrows = m_totalnumrows + m_counter_rows;
                    }

                    // <!-- Here we've alrady created the virtual buildings. Now we can update the building code -->
                    // <!-- this update should be done after virtual building creation because FR.00000.000 in req_labcode_mdm -->
                    // <!-- is the way to identiby virtual building creation -->


                    Ax.db.execute(`
                    UPDATE @load_francelis 
                    SET    req_labcode_mdm = 'FR.00' || req_entity[2,4] || '.' || 
                        CASE 
                                WHEN req_labcode = 'COV' 
                                AND    substr(req_labcode_mdm, 10) = '000' THEN '001' 
                                WHEN req_labcode = 'UEFA' 
                                AND    substr(req_labcode_mdm, 10) = '000' THEN '002' 
                                ELSE nvl(substr(req_labcode_mdm, 10), '999') 
                        END 
                    WHERE  req_labcode_mdm matches 'FR.00000.*'
                    `);                    

                    // ========= BUILDING MAPPING ============

                    m_processlog.log(`Loading table: labco_map_building`);

                    var rs_labco_map_building = new Ax.rs.Reader().db(options => {
                        // JLA corroborar campos de select
                        options.setResultSet(Ax.db.executeQuery(`
                            SELECT 
                                req_labcode_mdm build_cd,
                                req_entity ent_code,
                                '${labco_ftpconnect.con_code}' map_sourcesys,
                                req_labcode_mdm map_lcode,
                                MAX(req_labcode_name) map_lname
                            FROM @load_francelis
                            GROUP BY 1, 2, 3, 4
                        `));
                        // options.setColumnNameMapping(["servercode","pat_code"]);
                    })
                    .writer();
                    // ;
                    // console.log(rs_labco_map_building.rows().first(100));
                    // throw new Error("LLEGA ");
                    
                    rs_labco_map_building.db(options => {
                        // options.setLogger(console.getLogger());
                        options.setConnection(Ax.db.getObject());
                        options.setTableName("labco_map_building");
                        options.setTablePrimaryKeyColumns("map_sourcesys","map_lcode");
                        options.setBatchSize(5000);
                        // options.setInsertFirst(true);
                        options.setErrorHandler(error => {
                            var log_events = {
                                "log_id": 0,
                                "log_procnum": log_process.log_procnum,
                                "subsystem"   : p_processlog.getConCode(),
                                "event_type": "E0010",
                                "tablename": "labco_map_building",
                                "msglog1" : String(error.getData()),
                                "msglog2" : String (error.getMessage()),
                                "numlog1": error.getRow(),
                                // "numlog2"     : error.getColumn(),
                                // "numlog3"     : error.getSQLCode(),
                                "numlog4": String (error.getErrorCode())
                            };
                            Ax.db.insert("log_events", log_events);
    
                            // Continue ignoring error
                            return true;
                        });
                    });
                    m_counter = rs_labco_map_building.getStatistics();

                    // ========= PATIENT ============

                    m_processlog.log(`Loading table: local_masterpatient`);

                    var rs_local_masterpatient = new Ax.rs.Reader().db(options => {
                        // JLA corroborar campos de select
                        options.setResultSet(Ax.db.executeQuery(`
                        SELECT 
                            DISTINCT '${labco_ftpconnect.con_code}' servercode, pat_num_ssv pat_code, pat_nom_usage pat_firstname, pat_prenom pat_surname, pat_sex pat_sex
                        FROM @load_francelis
                        `));
                        // options.setColumnNameMapping(["servercode","pat_code"]);
                    })
                    .writer();
                    // ;
                    // console.log(rs_local_masterpatient.rows().first(100));
                    // throw new Error("LLEGA ");
    
                    rs_local_masterpatient.db(options => {
                        //options.setLogger(console.getLogger());
                        options.setConnection(Ax.db.getObject());

                        options.setTableName("local_masterpatient");
                        options.setBatchSize(5000);
                        // options.setInsertFirst(true);
                        options.setErrorHandler(error => {
                            var log_events = {
                                "log_id": 0,
                                "log_procnum": log_process.log_procnum,
                                "subsystem"   : p_processlog.getConCode(),
                                "event_type": "E0010",
                                "tablename": "labco_building",
                                "msglog1" : String(error.getData()),
                                "msglog2" : String (error.getMessage()),
                                "numlog1": error.getRow(),
                                // "numlog2"     : error.getColumn(),
                                // "numlog3"     : error.getSQLCode(),
                                "numlog4": String (error.getErrorCode())
                            };
                            Ax.db.insert("log_events", log_events);
    
                            // Continue ignoring error
                            return true;
                        });
                    });
                    m_counter = rs_local_masterpatient.getStatistics();

                    // ========= AMC - MUTUEL ============

                    m_processlog.log(`Loading table: local_masterpayer :: AMC - MUTUEL`);

                    var rs_local_masterpayer = new Ax.rs.Reader().db(options => {
                        // JLA corroborar campos de select
                        options.setResultSet(Ax.db.executeQuery(`
                            SELECT 
                                DISTINCT '${labco_ftpconnect.con_code}' servercode, 'AMC'||amc_code pay_code, amc_nom pay_name, amc_id pay_auxid
                            FROM @load_francelis
                        `));
                        // <filter>
                        //     <column name='pay_code'><string>AMC<pay_code/></string></column>
                        // </filter>
                        //Agregado en la select
                        // options.setColumnFilterMap("pay_code", (src) => {return "AMC".concat(src);});
                    })
                    .writer();
                    // ;
                    // console.log(rs_local_masterpayer.rows().first(100));
                    // throw new Error("LLEGA ");
    
                    rs_local_masterpayer.db(options => {
                        //options.setLogger(console.getLogger());
                        options.setConnection(Ax.db.getObject());
                        options.setTableName("local_masterpayer");
                        options.setBatchSize(5000);
                        // options.setInsertFirst(true);
                        options.setErrorHandler(error => {
                            var log_events = {
                                "log_id": 0,
                                "log_procnum": log_process.log_procnum,
                                "subsystem"   : p_processlog.getConCode(),
                                "event_type": "E0010",
                                "tablename": "local_masterpayer",
                                "msglog1" : String(error.getData()),
                                "msglog2" : String (error.getMessage()),
                                "numlog1": error.getRow(),
                                // "numlog2"     : error.getColumn(),
                                // "numlog3"     : error.getSQLCode(),
                                "numlog4": String (error.getErrorCode())
                            };
                            Ax.db.insert("log_events", log_events);
    
                            // Continue ignoring error
                            return true;
                        });
                    });
                    m_counter = rs_local_masterpayer.getStatistics();

                    // ========= AMO - SOCIAL SECURITY ============

                    m_processlog.log(`Loading table: local_masterpayer :: AMO - SOCIAL SECURITY`);

                    var rs_local_masterpayer_ss = new Ax.rs.Reader().db(options => {
                        // JLA corroborar campos de select
                        options.setResultSet(Ax.db.executeQuery(`
                            SELECT 
                                DISTINCT '${labco_ftpconnect.con_code}' servercode, 'AMO'||amo_code pay_code, amo_nom pay_name, amo_centre pay_aux01
                            FROM @load_francelis
                        `));
                        // <filter>
                        //     <column name='pay_code'><string>AMO<pay_code/></string></column>
                        // </filter>
                        //Add filter en select
                        // options.setColumnFilterMap("pay_code", (src) => {return "AMO".concat(src);});
                    })
                    .writer();
                    // ;
                    // console.log(rs_local_masterpayer_ss.rows().first(100));
                    // throw new Error("LLEGA ");                    
    
                    rs_local_masterpayer_ss.db(options => {
                        //options.setLogger(console.getLogger());
                        options.setConnection(Ax.db.getObject());

                        options.setTableName("local_masterpayer");
                        options.setBatchSize(5000);
                        // options.setInsertFirst(true);
                        options.setErrorHandler(error => {
                            var log_events = {
                                "log_id": 0,
                                "log_procnum": log_process.log_procnum,
                                "subsystem"   : p_processlog.getConCode(),
                                "event_type": "E0010",
                                "tablename": "local_masterpayer",
                                "msglog1" : String(error.getData()),
                                "msglog2" : String (error.getMessage()),
                                "numlog1": error.getRow(),
                                // "numlog2"     : error.getColumn(),
                                // "numlog3"     : error.getSQLCode(),
                                "numlog4": String (error.getErrorCode())
                            };
                            Ax.db.insert("log_events", log_events);
    
                            // Continue ignoring error
                            return true;
                        });
                    });
                    m_counter = rs_local_masterpayer_ss.getStatistics();

                    // ========= PRELEVEUR ============

                    m_processlog.log(`Loading table: local_mastersampler`);

                    var rs_local_mastersampler = new Ax.rs.Reader().db(options => {
                        // JLA corroborar campos de select
                        options.setResultSet(Ax.db.executeQuery(`
                            SELECT 
                                    DISTINCT '${labco_ftpconnect.con_code}' servercode, pre_code sam_code, pre_prenom || pre_nom sam_name, pre_rpps sam_auxid, pre_adeli col_aux01
                            FROM @load_francelis
                            WHERE 
                                    pre_code IS NOT NULL
                        `));
                        // options.setColumnNameMapping(["servercode","pat_code"]);
                    })
                    .writer();
                    // ;
                    // console.log(rs_local_mastersampler.rows().first(100));
                    // throw new Error("LLEGA ");                             
    
                    rs_local_mastersampler.db(options => {
                        //options.setLogger(console.getLogger());
                        options.setConnection(Ax.db.getObject());

                        options.setTableName("local_mastersampler");
                        options.setBatchSize(5000);
                        // options.setInsertFirst(true);
                        options.setErrorHandler(error => {
                            var log_events = {
                                "log_id": 0,
                                "log_procnum": log_process.log_procnum,
                                "subsystem"   : p_processlog.getConCode(),
                                "event_type": "E0010",
                                "tablename": "local_mastersampler",
                                "msglog1" : String(error.getData()),
                                "msglog2" : String (error.getMessage()),
                                "numlog1": error.getRow(),
                                // "numlog2"     : error.getColumn(),
                                // "numlog3"     : error.getSQLCode(),
                                "numlog4": String (error.getErrorCode())
                            };
                            Ax.db.insert("log_events", log_events);
    
                            // Continue ignoring error
                            return true;
                        });
                    });
                    m_counter = rs_local_mastersampler.getStatistics();


                    // ========= MASTER TESTORDER ============

                    m_processlog.log(`Loading table: local_mastertestorder`);

                    var rs_local_mastertestorder = new Ax.rs.Reader().db(options => {
                        // JLA se agrega "" atest_rdefaut x error de primary, test_name, verificar al cargar datos
                        options.setResultSet(Ax.db.executeQuery(`
                            SELECT 
                                    DISTINCT '${labco_ftpconnect.con_code}' servercode, code_analyse test_code, "" atest_rdefaut
                            FROM @load_francelis
                            WHERE 
                                    code_analyse IS NOT NULL
                        `));
                        // options.setColumnNameMapping(["servercode","pat_code"]);
                    })
                    .writer();
                    // ;
                    //Nada en frozen
                    // console.log(rs_local_mastertestorder.rows().first(100));
                    // throw new Error("LLEGA ");       
    
                    rs_local_mastertestorder.db(options => {
                        // options.setLogger(console.getLogger());
                        options.setConnection(Ax.db.getObject());
                        options.setTableName("local_mastertestorder");
                        // options.setTablePrimaryKeyColumns("servercode","test_code");
                        options.setBatchSize(5000);
                        // options.setInsertFirst(true);
                        options.setErrorHandler(error => {
                            var log_events = {
                                "log_id": 0,
                                "log_procnum": log_process.log_procnum,
                                "subsystem"   : p_processlog.getConCode(),
                                "event_type": "E0010",
                                "tablename": "local_mastertestorder",
                                "msglog1" : String(error.getData()),
                                "msglog2" : String (error.getMessage()),
                                "numlog1": error.getRow(),
                                // "numlog2"     : error.getColumn(),
                                // "numlog3"     : error.getSQLCode(),
                                "numlog4": String (error.getErrorCode())
                            };
                            Ax.db.insert("log_events", log_events);
    
                            // Continue ignoring error
                            return true;
                        });
                    });
                    m_counter = rs_local_mastertestorder.getStatistics();



                    // ========= MASTER REQUEST ============

                    // m_processlog.log(`Loading fact table: fact_int_request`);

                    // var rs_fact_int_request = new Ax.rs.Reader().db(options => {
                    //     options.setResultSet(Ax.db.executeQuery(`
                    //         SELECT DISTINCT 
                    //                         -- 0 req_id,  -- Serial no update coment 
                    //                         '${labco_ftpconnect.con_code}' s_sourcesys, 
                    //                         1                              s_loading, 
                    //                         req_entity, 
                    //                         '${labco_ftpconnect.con_code}' req_servercode, 
                    //                         req_labcode, 
                    //                         req_code, 
                    //                         CASE 
                    //                                         WHEN Nvl(req_patient_amount, 0) != 0 THEN pat_num_ssv 
                    //                                         ELSE NULL::CHAR 
                    //                         END req_payer1, 
                    //                         'AMO' 
                    //                                         || amo_code req_payer2, 
                    //                         'AMC' 
                    //                                         || amc_code req_payer3, 
                    //                         NULL::CHAR                  req_payer4, -- Correspondant 
                    //                         @load_francelis.pat_num_ssv req_patcode, 
                    //                         pre_rpps                    req_precod1, 
                    //                         local_masterpatient.id_code req_idpatient, 
                    //                         CASE 
                    //                                         WHEN nvl(req_patient_amount, 0) != 0 THEN local_masterpatient.id_code
                    //                                         ELSE NULL::INTEGER 
                    //                         END                                            req_idpayer1, 
                    //                         1                                              req_numreq, 
                    //                         0                                              req_countrykpi, 
                    //                         to_date(req_date_created, "%Y/%m/%d %H:%M:%S") req_date_created, 
                    //                         req_date_created[                              12,13] 
                    //                                         || req_date_created[           15,16] req_time_created, 
                    //                         today                                          req_date_loaded, 
                    //                         ${log_process.log_procnum}                     req_procnum, 
                    //                         ''                                             req_status, 
                    //                         'N'                                            req_delete, 
                    //                         nvl(req_amount_eur, 0)                         req_est_amount_eur, 
                    //                         nvl(req_amount_eur, 0)                         req_inv_amount_eur, 
                    //                         nvl(req_amount_eur, 0)                         req_est_amount_com, 
                    //                         nvl(req_amount_eur, 0)                         req_inv_amount_com, 
                    //                         nvl(req_patient_amount, 0)                     req_pay1_est_amount_eur, 
                    //                         nvl(req_patient_amount, 0)                     req_pay1_inv_amount_eur, 
                    //                         nvl(req_patient_amount, 0)                     req_pay1_est_amount_com, 
                    //                         nvl(req_patient_amount, 0)                     req_pay1_inv_amount_com, 
                    //                         nvl(req_amo_amount, 0)                         req_pay2_est_amount_eur, 
                    //                         nvl(req_amo_amount, 0)                         req_pay2_inv_amount_eur, 
                    //                         nvl(req_amo_amount, 0)                         req_pay2_est_amount_com, 
                    //                         nvl(req_amo_amount, 0)                         req_pay2_inv_amount_com, 
                    //                         nvl(req_amc_amount, 0)                         req_pay3_est_amount_eur, 
                    //                         nvl(req_amc_amount, 0)                         req_pay3_inv_amount_eur, 
                    //                         nvl(req_amc_amount, 0)                         req_pay3_est_amount_com, 
                    //                         nvl(req_amc_amount, 0)                         req_pay3_inv_amount_com, 
                    //                         nvl(req_corr_amount, 0)                        req_pay4_est_amount_eur, 
                    //                         nvl(req_corr_amount, 0)                        req_pay4_inv_amount_eur, 
                    //                         nvl(req_corr_amount, 0)                        req_pay4_est_amount_com, 
                    //                         nvl(req_corr_amount, 0)                        req_pay4_inv_amount_com, 
                    //                         'EUR'                                          currency 
                    //         FROM            @load_francelis , 
                    //                         OUTER local_masterpatient 
                    //         WHERE           '${labco_ftpconnect.con_code}' = local_masterpatient.servercode 
                    //         AND             @load_francelis.pat_num_ssv = local_masterpatient.pat_code
                    //     `));
                    //     // options.setColumnNameMapping(["servercode","pat_code"]);
                    // })
                    // .writer();
                    // // ;
                    // // console.log(rs_fact_int_request.rows().first(100));
                    // // throw new Error("LLEGA ");                       
    
                    // rs_fact_int_request.db(options => {
                    //     // options.setLogger(console.getLogger());
                    //     options.setConnection(Ax.db.getObject());

                    //     options.setTableName("fact_int_request");
                    //     options.setTablePrimaryKeyColumns("req_servercode","req_labcode","req_code");
                    //     options.setBatchSize(5000);
                    //     // options.setInsertFirst(true);
                    //     options.setErrorHandler(error => {
                    //         var log_events = {
                    //             "log_id": 0,
                    //             "log_procnum": log_process.log_procnum,
                    //             "subsystem"   : p_processlog.getConCode(),
                    //             "event_type": "E0030",
                    //             "tablename": "fact_int_request",
                    //             //"msglog1"     : error.getRecord(),
                    //             //"msglog2"     : error.getException(),
                    //             "numlog1": error.getRow(),
                    //             // "numlog2"     : error.getColumn(),
                    //             // "numlog3"     : error.getSQLCode(),
                    //             "numlog4": error.getErrorCode()
                    //         };
                    //         Ax.db.insert("log_events", log_events);
    
                    //         // Continue ignoring error
                    //         return true;
                    //     });
                    // });

                    // m_counter = rs_fact_int_request.getStatistics();

                    // if (m_counter != null && m_counter != -1) {
                    //     var m_counter_rows = m_counter.getInsertCount() + m_counter.getUpdateCount() + m_counter.getErrorCount();
                    //     var m_log_process_files = {
                    //         "log_regid": 0,
                    //         "log_procnum": log_process.log_procnum,
                    //         "log_tabname": "fact_int_request",
                    //         "date_created": m_date_created,
                    //         "date_finished": new Ax.sql.Date(),
                    //         "rows_file": m_counter_rows,
                    //         "rows_insert": m_counter.getInsertCount(),
                    //         "rows_update": m_counter.getUpdateCount(),
                    //         "rows_error": m_counter.getErrorCount()
                    //     };
                    //     Ax.db.insert("log_process_files", m_log_process_files);

                    //     var m_rowsloaded = m_counter_rows - m_counter.getErrorCount();
                    //     m_processlog.log(`Processed ${m_rowsloaded} row(s) of ${m_counter_rows} , ${m_counter.getErrorCount()} error(s)`);

                    //     if (Ax.db.isOnTransaction()) {
                    //         Ax.db.commitWork();
                    //     }

                    //     m_totalnumrows = m_totalnumrows + m_counter_rows;
                    // } else if (m_counter == -1) {
                        
                    //     var m_counter_rows = 0;
                    //     var m_log_process_files = {
                    //         "log_regid": 0,
                    //         "log_procnum": log_process.log_procnum,
                    //         "log_tabname": "fact_int_request",
                    //         "date_created": m_date_created,
                    //         "date_finished": new Ax.sql.Date(),
                    //         "rows_file": m_counter_rows,
                    //         "rows_insert": 0,
                    //         "rows_update": 0,
                    //         "rows_error": 0
                    //     };
                    //     Ax.db.insert("log_process_files", m_log_process_files);

                    //     var m_rowsloaded = 0;
                    //     m_processlog.log(`Processed ${m_rowsloaded} row(s) of ${m_counter_rows} , ${m_rowsloaded} error(s)`);

                    //     if (Ax.db.isOnTransaction()) {
                    //         Ax.db.commitWork();
                    //     }

                    //     m_totalnumrows = m_totalnumrows + m_counter_rows;

                    // }


                    // ================================MERGE REQUEST=========================================

                    // try{

                        m_processlog.log(`Loading fact table: fact_int_request`);
                        var m_date_created_request = new Ax.sql.Date();

                        m_processlog.log(`Creating temp table: @temp_load_request`);
                        //DISTINCT NOT NEEDED BECAUSE WE WILL FILTER BY ROW_NUMBER
                        Ax.db.execute(`
                            SELECT
                            ROW_NUMBER() OVER(PARTITION BY req_labcode_mdm, req_code) serialid, 
                                            -- 0 req_id,  -- Serial no update coment

                                            '${labco_ftpconnect.con_code}'::VARCHAR(15) req_servercode,
                                            -- req_labcode,
                                            req_labcode_mdm req_labcode,
                                            req_code, 

                                            '${labco_ftpconnect.con_code}' s_sourcesys, 
                                            1                              s_loading, 
                                            req_entity, 
                                            CASE 
                                                            WHEN Nvl(req_patient_amount, 0) != 0 THEN pat_num_ssv 
                                                            ELSE NULL::CHAR 
                                            END req_payer1, 
                                            'AMO' 
                                                            || amo_code req_payer2, 
                                            'AMC' 
                                                            || amc_code req_payer3, 
                                            NULL::CHAR                  req_payer4, -- Correspondant 
                                            -- @load_francelis.pat_num_ssv req_patcode, 
                                            pat_num_ssv req_patcode, 
                                            pre_rpps                    req_precod1, 
                                            local_masterpatient.id_code req_idpatient, 
                                            CASE 
                                                            WHEN nvl(req_patient_amount, 0) != 0 THEN local_masterpatient.id_code
                                                            ELSE NULL::INTEGER 
                                            END                                            req_idpayer1, 
                                            1                                              req_numreq, 
                                            0                                              req_countrykpi, 
                                            to_date(req_date_created, "%Y/%m/%d %H:%M:%S") req_date_created, 
                                            req_date_created[                              12,13] 
                                                            || req_date_created[           15,16] req_time_created, 
                                            today                                          req_date_loaded, 
                                            ${log_process.log_procnum}                     req_procnum, 
                                            ''                                             req_status, 
                                            'N'                                            req_delete, 
                                            nvl(req_amount_eur, 0)                         req_est_amount_eur, 
                                            nvl(req_amount_eur, 0)                         req_inv_amount_eur, 
                                            nvl(req_amount_eur, 0)                         req_est_amount_com, 
                                            nvl(req_amount_eur, 0)                         req_inv_amount_com, 
                                            nvl(req_patient_amount, 0)                     req_pay1_est_amount_eur, 
                                            nvl(req_patient_amount, 0)                     req_pay1_inv_amount_eur, 
                                            nvl(req_patient_amount, 0)                     req_pay1_est_amount_com, 
                                            nvl(req_patient_amount, 0)                     req_pay1_inv_amount_com, 
                                            nvl(req_amo_amount, 0)                         req_pay2_est_amount_eur, 
                                            nvl(req_amo_amount, 0)                         req_pay2_inv_amount_eur, 
                                            nvl(req_amo_amount, 0)                         req_pay2_est_amount_com, 
                                            nvl(req_amo_amount, 0)                         req_pay2_inv_amount_com, 
                                            nvl(req_amc_amount, 0)                         req_pay3_est_amount_eur, 
                                            nvl(req_amc_amount, 0)                         req_pay3_inv_amount_eur, 
                                            nvl(req_amc_amount, 0)                         req_pay3_est_amount_com, 
                                            nvl(req_amc_amount, 0)                         req_pay3_inv_amount_com, 
                                            nvl(req_corr_amount, 0)                        req_pay4_est_amount_eur, 
                                            nvl(req_corr_amount, 0)                        req_pay4_inv_amount_eur, 
                                            nvl(req_corr_amount, 0)                        req_pay4_est_amount_com, 
                                            nvl(req_corr_amount, 0)                        req_pay4_inv_amount_com, 
                                            'EUR'                                          currency 
                            FROM            @load_francelis, 
                                            OUTER local_masterpatient 
                            WHERE           '${labco_ftpconnect.con_code}' = local_masterpatient.servercode 
                            AND             @load_francelis.pat_num_ssv = local_masterpatient.pat_code

                            INTO TEMP @temp_load_request WITH NO LOG
                        `);

                        m_processlog.log(`Starting Indexes Creation for @temp_load_request`);

        
                        Ax.db.execute(`
                            CREATE INDEX @temp_load_request_i2
                        ON @temp_load_request (serialid);
                        `);
                        
                        Ax.db.execute(`
                            UPDATE STATISTICS LOW FOR TABLE @temp_load_request;
                        `);
        


                        m_processlog.log(`Starting Creation for @temp2_load_request`);
                        Ax.db.execute(`
                            SELECT * FROM @temp_load_request where serialid = 1 
                            INTO TEMP @temp2_load_request WITH NO LOG`);


                        Ax.db.execute(`
                            CREATE INDEX @temp2_load_request_i
                        ON @temp_load_request (req_servercode,req_labcode,req_code);
                        `);

                        m_processlog.log(`Starting Merge from @temp_load_request to fact_int_request`);
                        //EYH: The select used in the merge only have into account the first row by unique key of fact_int_testorder (source.serialid = 1)

                        //Test
                        // var rsx =  Ax.db.executeQuery(`SELECT count(distinct req_labcode,req_code) FROM @temp_load_request`);
                        // console.log(rsx);
                        
                        var rs_int_request = Ax.db.execute(`
                        MERGE INTO fact_int_request T
                        USING @temp2_load_request as S
                        ON(
                            T.req_servercode = S.req_servercode AND
                            T.req_labcode = S.req_labcode AND
                            T.req_code = S.req_code)
                        WHEN NOT MATCHED THEN
                        INSERT(
                                T.req_servercode,
                                T.req_labcode,
                                T.req_code, 
                                T.s_sourcesys, 
                                T.s_loading, 
                                T.req_entity, 
                                T.req_payer1, 
                                T.req_payer2, 
                                T.req_payer3, 
                                T.req_payer4,  
                                T.req_patcode, 
                                T.req_precod1, 
                                T.req_idpatient, 
                                T.req_idpayer1, 
                                T.req_numreq, 
                                T.req_countrykpi, 
                                T.req_date_created, 
                                T.req_time_created, 
                                T.req_date_loaded, 
                                T.req_procnum, 
                                T.req_status, 
                                T.req_delete, 
                                T.req_est_amount_eur, 
                                T.req_inv_amount_eur, 
                                T.req_est_amount_com, 
                                T.req_inv_amount_com, 
                                T.req_pay1_est_amount_eur, 
                                T.req_pay1_inv_amount_eur, 
                                T.req_pay1_est_amount_com, 
                                T.req_pay1_inv_amount_com, 
                                T.req_pay2_est_amount_eur, 
                                T.req_pay2_inv_amount_eur, 
                                T.req_pay2_est_amount_com, 
                                T.req_pay2_inv_amount_com, 
                                T.req_pay3_est_amount_eur, 
                                T.req_pay3_inv_amount_eur, 
                                T.req_pay3_est_amount_com, 
                                T.req_pay3_inv_amount_com, 
                                T.req_pay4_est_amount_eur, 
                                T.req_pay4_inv_amount_eur, 
                                T.req_pay4_est_amount_com, 
                                T.req_pay4_inv_amount_com, 
                                T.currency 
                        )
                        VALUES(

                                S.req_servercode,
                                S.req_labcode,
                                S.req_code, 
                                S.s_sourcesys, 
                                S.s_loading, 
                                S.req_entity, 
                                S.req_payer1, 
                                S.req_payer2, 
                                S.req_payer3, 
                                S.req_payer4,  
                                S.req_patcode, 
                                S.req_precod1, 
                                S.req_idpatient, 
                                S.req_idpayer1, 
                                S.req_numreq, 
                                S.req_countrykpi, 
                                S.req_date_created, 
                                S.req_time_created, 
                                S.req_date_loaded, 
                                S.req_procnum, 
                                S.req_status, 
                                S.req_delete, 
                                S.req_est_amount_eur, 
                                S.req_inv_amount_eur, 
                                S.req_est_amount_com, 
                                S.req_inv_amount_com, 
                                S.req_pay1_est_amount_eur, 
                                S.req_pay1_inv_amount_eur, 
                                S.req_pay1_est_amount_com, 
                                S.req_pay1_inv_amount_com, 
                                S.req_pay2_est_amount_eur, 
                                S.req_pay2_inv_amount_eur, 
                                S.req_pay2_est_amount_com, 
                                S.req_pay2_inv_amount_com, 
                                S.req_pay3_est_amount_eur, 
                                S.req_pay3_inv_amount_eur, 
                                S.req_pay3_est_amount_com, 
                                S.req_pay3_inv_amount_com, 
                                S.req_pay4_est_amount_eur, 
                                S.req_pay4_inv_amount_eur, 
                                S.req_pay4_est_amount_com, 
                                S.req_pay4_inv_amount_com, 
                                S.currency                         
                        )
                        WHEN MATCHED THEN 
                        UPDATE SET
                                T.s_sourcesys  = S.s_sourcesys, 
                                T.s_loading  = S.s_loading, 
                                T.req_entity  = S.req_entity, 
                                T.req_payer1  = S.req_payer1, 
                                T.req_payer2  = S.req_payer2, 
                                T.req_payer3  = S.req_payer3, 
                                T.req_payer4   = S.req_payer4,  
                                T.req_patcode  = S.req_patcode, 
                                T.req_precod1  = S.req_precod1, 
                                T.req_idpatient  = S.req_idpatient, 
                                T.req_idpayer1  = S.req_idpayer1, 
                                T.req_numreq  = S.req_numreq, 
                                T.req_countrykpi  = S.req_countrykpi, 
                                T.req_date_created  = S.req_date_created, 
                                T.req_time_created  = S.req_time_created, 
                                T.req_date_loaded  = S.req_date_loaded, 
                                T.req_procnum  = S.req_procnum, 
                                T.req_status  = S.req_status, 
                                T.req_delete  = S.req_delete, 
                                T.req_est_amount_eur  = S.req_est_amount_eur, 
                                T.req_inv_amount_eur  = S.req_inv_amount_eur, 
                                T.req_est_amount_com  = S.req_est_amount_com, 
                                T.req_inv_amount_com  = S.req_inv_amount_com, 
                                T.req_pay1_est_amount_eur  = S.req_pay1_est_amount_eur, 
                                T.req_pay1_inv_amount_eur  = S.req_pay1_inv_amount_eur, 
                                T.req_pay1_est_amount_com  = S.req_pay1_est_amount_com, 
                                T.req_pay1_inv_amount_com  = S.req_pay1_inv_amount_com, 
                                T.req_pay2_est_amount_eur  = S.req_pay2_est_amount_eur, 
                                T.req_pay2_inv_amount_eur  = S.req_pay2_inv_amount_eur, 
                                T.req_pay2_est_amount_com  = S.req_pay2_est_amount_com, 
                                T.req_pay2_inv_amount_com  = S.req_pay2_inv_amount_com, 
                                T.req_pay3_est_amount_eur  = S.req_pay3_est_amount_eur, 
                                T.req_pay3_inv_amount_eur  = S.req_pay3_inv_amount_eur, 
                                T.req_pay3_est_amount_com  = S.req_pay3_est_amount_com, 
                                T.req_pay3_inv_amount_com  = S.req_pay3_inv_amount_com, 
                                T.req_pay4_est_amount_eur  = S.req_pay4_est_amount_eur, 
                                T.req_pay4_inv_amount_eur  = S.req_pay4_inv_amount_eur, 
                                T.req_pay4_est_amount_com  = S.req_pay4_est_amount_com, 
                                T.req_pay4_inv_amount_com  = S.req_pay4_inv_amount_com, 
                                T.currency  = S.currency
                        `);
                        
                        m_processlog.log('Merge finished');                    

                        //m_counter = rs_int_request.getStatistics();
                        // EYH: It seems the merge statement only return the rows affected without diference between insert or update 
                        m_counter = rs_int_request;
                        var m_log_process_files = {
                        "log_regid"       : 0,
                        "log_procnum"     : log_process.log_procnum,
                        "log_tabname"     : 'fact_int_request',
                        "date_created"    : m_date_created_request,
                        "date_finished"   : new Ax.sql.Date(),
                        "rows_file"       : m_counter.count,
                        "rows_insert"     : m_counter.count,
                        "rows_update"     : m_counter.count,
                        "rows_error"      : 0
                        };
                        
                        Ax.db.insert("log_process_files", m_log_process_files);

                        if (Ax.db.isOnTransaction()) { Ax.db.commitWork(); }

                    // } catch (Err) {
                    //             var log_events = {
                    //             "log_id": 0,
                    //             "log_procnum": log_process.log_procnum,
                    //             "subsystem"   : p_processlog.getConCode(),
                    //             "event_type": "E0030",
                    //             "tablename": "fact_int_request",
                    //             //"msglog1"     : error.getRecord(),
                    //             //"msglog2"     : error.getException(),
                    //             // "numlog1": error.getRow(),
                    //             // "numlog2"     : error.getColumn(),
                    //             // "numlog3"     : error.getSQLCode(),
                    //             // "numlog4": error.getErrorCode()
                    //         };
                    //         Ax.db.insert("log_events", log_events);
                    // }
                    

                    // ========= MASTER TESTORDER ============

                    // m_processlog.log(`Loading fact table: fact_int_testorder`);
                    // m_processlog.log(log_process.log_procnum + " "+p_processlog.getConCode());

                    // var rs_fact_int_testorder = new Ax.rs.Reader().db(options => {
                    // options.setResultSet(Ax.db.executeQuery(`
                    //     SELECT 
                    //         0                              test_id, 
                    //         0                              req_id, 
                    //         '${labco_ftpconnect.con_code}' s_sourcesys, 
                    //         req_entity                     entity, 
                    //         '${labco_ftpconnect.con_code}' servercode, 
                    //         req_code                       requestcode, 
                    //         code_analyse                   testcode, 
                    //         1                              orderindex, 
                    //         req_labcode                    labcode, 
                    //         1                              quantity, 
                    //         CASE 
                    //                 WHEN Nvl(req_patient_amount, 0) != 0 THEN pat_num_ssv 
                    //                 ELSE NULL::char 
                    //         END test_payer1, 
                    //         'AMO' || amo_code test_payer2, 
                    //         'AMC' || amc_code test_payer3, 
                    //         NULL::char         test_payer4, 
                    //         CASE 
                    //                 WHEN nvl(req_patient_amount, 0) != 0 THEN local_masterpatient.id_code 
                    //                 ELSE NULL::integer 
                    //         END                                            test_idpayer1, 
                    //         to_date(req_date_created, "%Y/%m/%d %H:%M:%S") date_created, 
                    //         req_date_created[12,13] || req_date_created[15,16] time_created 
                    //     FROM   @load_francelis , 
                    //         OUTER local_masterpatient 
                    //     WHERE  '${labco_ftpconnect.con_code}' = local_masterpatient.servercode 
                    //     AND    @load_francelis.pat_num_ssv = local_masterpatient.pat_code


                    //     `));
                    //     // options.setColumnNameMapping(["servercode","pat_code"]);

                    //     options.setColumnType("test_id", Ax.sql.Types.INTEGER);
                    //     options.setColumnType("req_id", Ax.sql.Types.INTEGER );
                    //     options.setColumnType("s_sourcesys", Ax.sql.Types.VARCHAR);
                    //     options.setColumnType("entity", Ax.sql.Types.VARCHAR);
                    //     options.setColumnType("servercode", Ax.sql.Types.VARCHAR);
                    //     options.setColumnType("labcode", Ax.sql.Types.VARCHAR);
                    //     options.setColumnType("requestcode", Ax.sql.Types.VARCHAR);
                    //     options.setColumnType("testcode", Ax.sql.Types.VARCHAR);
                    //     options.setColumnType("orderindex", Ax.sql.Types.INTEGER);
                    //     options.setColumnType("quantity", Ax.sql.Types.INTEGER);
                    //     options.setColumnType("test_payer1", Ax.sql.Types.VARCHAR);
                    //     options.setColumnType("test_payer2", Ax.sql.Types.VARCHAR);
                    //     options.setColumnType("test_payer3", Ax.sql.Types.VARCHAR);
                    //     options.setColumnType("test_payer4", Ax.sql.Types.VARCHAR);
                    //     options.setColumnType("test_idpayer1", Ax.sql.Types.INTEGER);
                    //     options.getFormats().setParser("date_created", Ax.rs.DataParser.ofSQLDate("ddMMyyyy"));
                    //     options.setColumnType("time_created", Ax.sql.Types.CHAR);
                    //     options.setMemoryMapped(true);
                    // })
                    // .writer();
                    // // ;
                    // // console.log(rs_fact_int_testorder.rows().first(10));
                    // // throw new Error("LLEGA ");

                    // rs_fact_int_testorder.db(options => {
                        
                    //     // options.setLogger(console.getLogger());
                    //     options.setConnection(Ax.db.getObject());
                    //     options.setTableName("fact_int_testorder");
                    //     // options.setTablePrimaryKeyColumns("servercode","testcode","requestcode","orderindex","labcode");
                    //     // options.setBatchSize(5000);
                    //     options.setInsertFirst(true);
                    //     options.setErrorHandler(error => {
                            
                    //         var log_events = {
                    //             "log_id": 0,
                    //             "log_procnum": log_process.log_procnum,
                    //             "subsystem"   : p_processlog.getConCode(),
                    //             "event_type": "E0030",
                    //             "tablename": "fact_int_testorder",
                    //             //"msglog1"     : error.getRecord(),
                    //             //"msglog2"     : error.getException(),
                    //             "numlog1": error.getRow(),
                    //             // "numlog2"     : error.getColumn(),
                    //             // "numlog3"     : error.getSQLCode(),
                    //             "numlog4": error.getErrorCode()
                    //         };
                            
                    //         Ax.db.insert("log_events", log_events);
    
                    //         // Continue ignoring error
                    //         // return true;
                    //         return false;
                    //     });
                    // });
                    // m_counter = rs_fact_int_testorder.getStatistics();

                    // if (m_counter != null && m_counter != -1) {
                    //     var m_counter_rows = m_counter.getInsertCount() + m_counter.getUpdateCount() + m_counter.getErrorCount();
                    //     var m_log_process_files = {
                    //         "log_regid": 0,
                    //         "log_procnum": log_process.log_procnum,
                    //         "log_tabname": "fact_int_testorder",
                    //         "date_created": m_date_created,
                    //         "date_finished": new Ax.sql.Date(),
                    //         "rows_file": m_counter_rows,
                    //         "rows_insert": m_counter.getInsertCount(),
                    //         "rows_update": m_counter.getUpdateCount(),
                    //         "rows_error": m_counter.getErrorCount()
                    //     };
                    //     Ax.db.insert("log_process_files", m_log_process_files);

                    //     var m_rowsloaded = m_counter_rows - m_counter.getErrorCount();
                    //     m_processlog.log(`Processed ${m_rowsloaded} row(s) of ${m_counter_rows} , ${m_counter.getErrorCount()} error(s)`);

                    //     if (Ax.db.isOnTransaction()) {
                    //         Ax.db.commitWork();
                    //     }

                    //     m_totalnumrows = m_totalnumrows + m_counter_rows;
                    // } else if (m_counter == -1) {
                        
                    //     var m_counter_rows = 0;
                    //     var m_log_process_files = {
                    //         "log_regid": 0,
                    //         "log_procnum": log_process.log_procnum,
                    //         "log_tabname": "fact_int_testorder",
                    //         "date_created": m_date_created,
                    //         "date_finished": new Ax.sql.Date(),
                    //         "rows_file": m_counter_rows,
                    //         "rows_insert": 0,
                    //         "rows_update": 0,
                    //         "rows_error": 0
                    //     };
                    //     Ax.db.insert("log_process_files", m_log_process_files);

                    //     var m_rowsloaded = 0;
                    //     m_processlog.log(`Processed ${m_rowsloaded} row(s) of ${m_counter_rows} , ${m_rowsloaded} error(s)`);

                    //     if (Ax.db.isOnTransaction()) {
                    //         Ax.db.commitWork();
                    //     }

                    //     m_totalnumrows = m_totalnumrows + m_counter_rows;
                    // }

                    
                    // ================================MERGE TESTORDER=========================================

//                    try{

                        m_processlog.log(`Loading fact table: fact_int_testorder`);
                        var m_date_created_testorder = new Ax.sql.Date();

                        m_processlog.log(`Creating temp table: @temp_load_testorder`);
                        Ax.db.execute(`
                            SELECT                          
                            ROW_NUMBER() OVER(PARTITION BY req_labcode_mdm, req_code, code_analyse ) serialid, 

                                '${labco_ftpconnect.con_code}'::VARCHAR(15) servercode, 
                                req_code                       requestcode, 
                                code_analyse                   testcode, 
                                1                              orderindex, 
                                -- req_labcode                    labcode, 
                                req_labcode_mdm                labcode,
                                0                              test_id, 
                                0                              req_id, 
                                '${labco_ftpconnect.con_code}' s_sourcesys, 
                                req_entity                     entity, 

                                1                              quantity, 
                                CASE 
                                        WHEN Nvl(req_patient_amount, 0) != 0 THEN pat_num_ssv 
                                        ELSE NULL::char 
                                END test_payer1, 
                                'AMO' || amo_code test_payer2, 
                                'AMC' || amc_code test_payer3, 
                                NULL::char         test_payer4, 
                                CASE 
                                        WHEN nvl(req_patient_amount, 0) != 0 THEN local_masterpatient.id_code 
                                        ELSE NULL::integer 
                                END                                            test_idpayer1, 
                                to_date(req_date_created, "%Y/%m/%d %H:%M:%S") date_created, 
                                req_date_created[12,13] || req_date_created[15,16] time_created 
                            FROM   @load_francelis , 
                                OUTER local_masterpatient 
                            WHERE  '${labco_ftpconnect.con_code}' = local_masterpatient.servercode 
                            AND    @load_francelis.pat_num_ssv = local_masterpatient.pat_code                        

                            INTO TEMP @temp_load_testorder WITH NO LOG
                        `);

                        m_processlog.log(`Starting Indexes Creation for @temp_load_testorder`);
        
                        Ax.db.execute(`
                            CREATE INDEX @temp_load_testorder_i
                        ON @temp_load_testorder (serialid);
                        `);
                        

                        m_processlog.log(`Creating temp table: @tmp_fact_int_testorder`);
                        Ax.db.execute(`
                            SELECT * FROM @temp_load_testorder where serialid = 1 
                            INTO TEMP @tmp_fact_int_testorder WITH NO LOG`);

                        Ax.db.execute(`
                            CREATE INDEX @tmp_fact_int_testorder_i
                        ON @tmp_fact_int_testorder (servercode, requestcode, testcode, orderindex, labcode);
                        `);

                        
                        //test
                        // var rsx =  Ax.db.executeQuery(`SELECT count(*) FROM @temp_load_testorder`);
                        // console.log(rsx);

                        m_processlog.log(`Starting Merge from @temp_load_testorder to fact_int_testorder`);
                        //EYH: The select used in the merge only have into account the first row by unique key of fact_int_testorder (source.serialid = 1)

                         var rs_int_testorder = Ax.db.execute(`
                        MERGE INTO fact_int_testorder T
                        USING 
                        @tmp_fact_int_testorder as S
                        ON(
                            T.servercode = S.servercode AND
                            T.requestcode = S.requestcode AND
                            T.testcode = S.testcode AND
                            T.orderindex = S.orderindex AND
                            T.labcode = S.labcode
                            )
                        WHEN NOT MATCHED THEN
                        INSERT(
                                T.servercode, 
                                T.requestcode, 
                                T.testcode, 
                                T.orderindex, 
                                T.labcode, 
                                
                                T.test_id, 
                                T.req_id, 
                                T.s_sourcesys, 
                                T.entity, 
                                
                                T.quantity, 
                                T.test_payer1, 
                                T.test_payer2, 
                                T.test_payer3, 
                                T.test_payer4, 
                                T.test_idpayer1, 
                                T.date_created, 
                                T.time_created 
                        )
                        VALUES(
                                S.servercode, 
                                S.requestcode, 
                                S.testcode, 
                                S.orderindex, 
                                S.labcode, 
                                
                                S.test_id, 
                                S.req_id, 
                                S.s_sourcesys, 
                                S.entity, 
                                
                                S.quantity, 
                                S.test_payer1, 
                                S.test_payer2, 
                                S.test_payer3, 
                                S.test_payer4, 
                                S.test_idpayer1, 
                                S.date_created, 
                                S.time_created                                                  
                        )
                        WHEN MATCHED THEN 
                        UPDATE SET
                                T.test_id  = S.test_id, 
                                T.req_id  = S.req_id, 
                                T.s_sourcesys  = S.s_sourcesys, 
                                T.entity  = S.entity, 
                                
                                T.quantity  = S.quantity, 
                                T.test_payer1  = S.test_payer1, 
                                T.test_payer2  = S.test_payer2, 
                                T.test_payer3  = S.test_payer3, 
                                T.test_payer4  = S.test_payer4, 
                                T.test_idpayer1  = S.test_idpayer1, 
                                T.date_created  = S.date_created, 
                                T.time_created  = S.time_created 
                        
                        `);
                        

                        m_processlog.log('Merge finished');                    

                        //m_counter = rs_int_request.getStatistics();
                        // EYH: It seems the merge statement only return the rows affected without diference between insert or update 
                        m_counter = rs_int_testorder;
                        var m_log_process_files = {
                        "log_regid"       : 0,
                        "log_procnum"     : log_process.log_procnum,
                        "log_tabname"     : 'fact_int_testorder',
                        "date_created"    : m_date_created_testorder,
                        "date_finished"   : new Ax.sql.Date(),
                        "rows_file"       : m_counter.count,
                        "rows_insert"     : m_counter.count,
                        "rows_update"     : m_counter.count,
                        "rows_error"      : 0
                        };
                        
                        Ax.db.insert("log_process_files", m_log_process_files);

                        if (Ax.db.isOnTransaction()) { Ax.db.commitWork(); }                        

                    // } catch (Err) {
                    //         var log_events = {
                    //             "log_id": 0,
                    //             "log_procnum": log_process.log_procnum,
                    //             "subsystem"   : p_processlog.getConCode(),
                    //             "event_type": "E0030",
                    //             "tablename": "fact_int_testorder",
                    //             //"msglog1"     : error.getRecord(),
                    //             //"msglog2"     : error.getException(),
                    //             // "numlog1": error.getRow(),
                    //             // "numlog2"     : error.getColumn(),
                    //             // "numlog3"     : error.getSQLCode(),
                    //             // "numlog4": error.getErrorCode()
                    //         };
                    //         Ax.db.insert("log_events", log_events);
                    // }

                    // <!-- If all loaded properlly - Rename folder ane mark it LOADED -->
                    // <if>
                    //     <expr>
                    //         <not><local_rename2loaded><m_ftpDirFile /></local_rename2loaded></not>
                    //     </expr>
                    //     <then>
                    //         <exception>Failed to rename <m_ftpDirFile /></exception>
                    //     </then>
                    // </if>

                    // m_FileSetDir.renameTo(m_FileSetDir.getName()  + new Ax.util.Date().format("yyyyMMdd_HHmmss") + "_"+ ".LOADED");
                    
                    m_FileSetDir.renameTo( new Ax.util.Date().format("yyyyMMdd_HHmmss") +"_"+ m_FileSetDir.getName() + ".LOADED");

                    var renamed = new Ax.util.Date().format("yyyyMMdd_HHmmss") +"_"+ m_FileSetDir.getName() + ".LOADED"
                    console.log(`Rename from `+m_con_folder_path+`/`+m_dirname+` to `+ m_con_folder_path+`/`+ renamed);


                    // Update the processs log
                    Ax.db.update("log_process", {
                        "load_status": 1,
                        "date_finished": new Ax.sql.Date()
                    }, {
                        "log_procnum": log_process.log_procnum
                    });

                    // END Foreach folder that matches pattern (Foreach day with data) 

                    Ax.db.update("labco_ftpconnect", {
                        "con_loadstat": 0,
                        "con_loadend": new Ax.sql.Date(),
                        "con_errmsg": ""
                    }, {
                        "con_code": labco_ftpconnect.con_code
                    });


                
                }
                
            // } catch (Err) {
            //     /*
            //     <local_log>
            //         <labco_ftpconnect_con_code/>
            //         <string/>
            //         <string>Exception catched. Unmark loading status. Finish.</string>
            //     </local_log>
            //     */
            //     Ax.db.update("labco_ftpconnect", {
            //         "con_loadstat": 0,
            //         "con_loadend": new Ax.sql.Date(),
            //         "con_lastcon": null,
            //         "con_errmsg": Err.message,
            //         "con_errnum": labco_ftpconnect.con_errnum + 1
            //     }, {
            //         "con_code": labco_ftpconnect.con_code
            //     });
            //     if (log_process.log_procnum != null) {
            //         Ax.db.update("log_process", {
            //             "load_status": -1,
            //             "load_lasterr": Err.message,
            //             "date_finished": new Ax.sql.Date()
            //         }, {
            //             "log_procnum": log_process.log_procnum
            //         });
            //     }
            //     if (Ax.db.isOnTransaction()) {
            //         Ax.db.commitWork();
            //     }
            //     Ax.db.beginWork();

            //     console.log(`Error on loading process: ${Err.message}`);
            //     console.log(`Error on loading process: ${Err.stackTrace}`);
            // }

            Ax.db.execute(`DROP TABLE IF EXISTS @load_francelis`);
        

        }

        console.log(`Terminated processing of labco_ftpconnect.con_code  MATCHES '${p_source}'`);


}
