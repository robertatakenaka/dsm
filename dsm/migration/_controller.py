from dsm.migration import db


def create_mininum_record_in_isis_doc(pid, isis_updated_date):
    """
    Create a record in isis_doc only with pid, isis_updated_date and
    status == "pending_migration"
    """
    isis_document = (
        db.fetch_isis_document(pid) or
        db.create_isis_document()
    )
    if isis_document.isis_updated_date != isis_updated_date:
        isis_document._id = pid
        isis_document.isis_updated_date = isis_updated_date
        isis_document.update_status("PENDING_MIGRATION")
        db.save_data(isis_document)
        return {"pid": pid, "result": "done"}
    return {"pid": pid, "result": "skip"}
