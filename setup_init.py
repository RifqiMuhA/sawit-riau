import os
import shutil
import glob

base_dir = r"d:\Kuliah\STIS\Semester 6\4 - Teknologi Perekayasaan Data\Project"
data_dir = os.path.join(base_dir, "data-perusahaan")

db_map = {
    "db_kampar": "a-01-kampar",
    "db_pelalawan": "a-02-pelalawan",
    "db_siak": "a-03-siak",
    "db_indragiri_hulu": "b-04-indragiri-hulu",
    "db_kuantan_singingi": "b-05-kuansing",
    "db_indragiri_hilir": "b-06-indragiri-hilir",
    "db_bengkalis": "c-07-bengkalis",
    "db_rokan_hilir": "c-08-rokan-hilir",
    "db_kepulauan_meranti": "c-09-meranti",
    "db_rokan_hulu": "d-10-rokan-hulu",
    "db_pekanbaru": "d-11-pekanbaru",
    "db_dumai": "d-12-dumai"
}

init_mysql = os.path.join(base_dir, "init-sql", "mysql")
init_pg = os.path.join(base_dir, "init-sql", "postgres")
init_mongo = os.path.join(base_dir, "init-sql", "mongodb") # Wait, mongo json goes to data-perusahaan, not init-sql/mongodb. The init script is in init-sql/mongodb.
# Actually, looking at docker-compose, mongo mounts `./data-perusahaan:/data-perusahaan:ro` and expects json there.

def process_files():
    folders = glob.glob(os.path.join(data_dir, "Kab_*"))
    for folder in folders:
        if not os.path.isdir(folder): continue
        
        for f in os.listdir(folder):
            fpath = os.path.join(folder, f)
            if "Schema_MySQL" in f:
                target = os.path.join(init_mysql, f)
                with open(fpath, 'r', encoding='utf-8') as file:
                    content = file.read()
                for old, new in db_map.items():
                    content = content.replace(old, new)
                with open(target, 'w', encoding='utf-8') as file:
                    file.write(content)
            elif "Schema_Postgres" in f:
                target = os.path.join(init_pg, f)
                with open(fpath, 'r', encoding='utf-8') as file:
                    content = file.read()
                for old, new in db_map.items():
                    content = content.replace(old, new)
                with open(target, 'w', encoding='utf-8') as file:
                    file.write(content)
            elif "log_alert_harian" in f or "Laporan_Excel" in f:
                # json and xlsx go to data_dir
                target = os.path.join(data_dir, f)
                shutil.copy2(fpath, target)
        
        # After processing all files in folder, remove it
        shutil.rmtree(folder)
    
    print("Reorganization and DB renaming complete.")

if __name__ == "__main__":
    process_files()
