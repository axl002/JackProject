

//item schema for jackson
// includes seller info
public class Item{

    public String lastSeller;
    public String lastStashID;
    public String x;
    public String y;
    public int ilvl;
    public String icon;
    public String League;
    public String itemID;
    public String name;
    public String typeLine;
    public String note;
    public boolean identified;
    public boolean verified;
    public double price;
    //public RawFormat.PropertyList propertyList;
    public String explicitMods;
    public String flavorText;

    public Item(){
    }


    // method for extracting price from note
    static double parseValue(String note){
        String dankNote = note.toLowerCase();
        // check if buyout
        if(dankNote.contains("~b/o".toLowerCase())){
            if (dankNote.contains("chaos")){
                return Double.parseDouble(dankNote.replaceAll("[^0-9]+", ""))/72;
            }
            else if (dankNote.contains("exa")){
                return Double.parseDouble(dankNote.replaceAll("[^0-9]+", ""));
            }
            else {
                return -1;
            }
        }
        return -2;
    }


    // print out useful info
    public String toString(){
        return "Name: " + getName() + getTypeLine() + "\nPrice: " + getPrice() + "\nID: " + getItemID() + "\nLastOwner: " + getLastSeller() + " " + getLastStashID() + "\nMods" + printExplicitMods();
    }

    public String printExplicitMods(){
        return explicitMods;
    }


    public String getLastSeller(){
        return lastSeller;
    }
    public void setLastSeller(String lastSeller){
        this.lastSeller = lastSeller;
    }

    public String getLastStashID(){
        return lastStashID;
    }

    public void setLastStashID(String lastStashID){
        this.lastStashID = lastStashID;
    }
    public double getPrice(){
        return price;
    }

    // price is determined by note, do not allow directly setting price
    private void setPrice(String priceNote){
        price = parseValue(priceNote);
    }
    public String getX() {
        return x;
    }

    public void setX(String x) {
        this.x = x;
    }

    public String getY() {
        return y;
    }

    public void setY(String y) {
        this.y = y;
    }

    public int getIlvl() {
        return ilvl;
    }

    public void setIlvl(int ilvl) {
        this.ilvl = ilvl;
    }

    public String getIcon() {
        return icon;
    }

    public void setIcon(String icon) {
        this.icon = icon;
    }

    public String getLeague() {
        return League;
    }

    public void setLeague(String league) {
        League = league;
    }

    public String getItemID() {
        return itemID;
    }

    public void setItemID(String itemID) {
        this.itemID = itemID;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
        setPrice(note);
    }

    public boolean isIdentified() {
        return identified;
    }

    public void setIdentified(boolean identified) {
        this.identified = identified;
    }

    public boolean isVerified() {
        return verified;
    }

    public void setVerified(boolean verified) {
        this.verified = verified;
    }

    public String getExplicitMods() {
        return explicitMods;
    }

    public void setExplicitMods(String explicitMods) {
        this.explicitMods = explicitMods;
    }

    public String getFlavorText() {
        return flavorText;
    }

    public void setFlavorText(String flavorText) {
        this.flavorText = flavorText;
    }

    public String getInventoryID() {
        return inventoryID;
    }

    public void setInventoryID(String inventoryID) {
        this.inventoryID = inventoryID;
    }

    public String inventoryID;

    public String getTypeLine(){
        return this.typeLine;
    }

    public void setTypeLine(String typeLine){
        this.typeLine = typeLine;
    }



}

