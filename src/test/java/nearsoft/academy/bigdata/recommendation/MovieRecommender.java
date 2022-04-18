package nearsoft.academy.bigdata.recommendation;

//Mahout recommender libraries
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

//File Manipulation Libraries
import java.io.*;
import java.util.*;
import java.util.zip.*;

public class MovieRecommender {
    //HashMaps for storing IDs pair values(StringID=>NumericID) for Users and Products
    HashMap<String, Integer> usersIDStorage = new HashMap<String, Integer>();
    HashMap<String, Integer> productsIDStorage = new HashMap<String, Integer>();
    int totalReviews = 0;
    //Set productionMode to true if you want to run the test with pre-processed files(movies.csv,productsID.csv,usersID.csv,reviewsNumber.csv)
    //Set productionMode to false if you want to run the test with the full dataset and build the processed CSV files
    boolean productionMode = true; 

    public MovieRecommender(String filename) throws IOException{    
        //Processing movies.txt.gz for extracting userId,productId,score fields
        InputStream inputStream = new GZIPInputStream(new FileInputStream(filename));
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        String strCurrentLine;

        //Building the CSV output file
        File moviesData = new File("data/movies.csv");
        FileWriter writer = new FileWriter(moviesData);
        BufferedWriter lineWritter = new BufferedWriter(writer);

        //Expected patterns for the data in the movies dataset
        String userPattern = "review/userId: ";
        String prodPattern = "product/productId: ";
        String scorePattern = "review/score: ";

        //Aux variables for temporal fields storage
        String product = "";
        int productCont = 0;
        String user = "";
        int userCont = 0;
        String score = "";

        //Reading the file line by line and extracting the required information
        while((strCurrentLine = br.readLine()) != null){
            if(strCurrentLine.contains(prodPattern)) {
                product = strCurrentLine.substring(prodPattern.length());
                if(!this.productsIDStorage.containsKey(product)){
                    this.productsIDStorage.put(product, productCont);
                    productCont++;
                }
            }
            if(strCurrentLine.contains(userPattern)) {
                user = strCurrentLine.substring(userPattern.length());
                if(!this.usersIDStorage.containsKey(user)){
                    this.usersIDStorage.put(user, userCont);
                    userCont++;
                }
            }
            if(strCurrentLine.contains(scorePattern)) {
                this.totalReviews++;
                score = strCurrentLine.substring(scorePattern.length());
                lineWritter.write(this.usersIDStorage.get(user) + "," + this.productsIDStorage.get(product) + "," + score + "\n");
            }
        }
        lineWritter.close();
        writer.close();
        br.close();
        inputStream.close();    
    }

    //Method for recovering the StringID from the products hashmap using the associated numeric value
    public String getProductStringID(int val){
        for(String key : this.productsIDStorage.keySet()){
            if(this.productsIDStorage.get(key) == val){
                return key;
            }
        }
        return null;
    }

    //Methods for retrieving dataset properties
    public int getTotalReviews(){
        return this.totalReviews;
    }

    public int getTotalProducts(){
        return this.productsIDStorage.size();
    }

    public int getTotalUsers(){
        return this.usersIDStorage.size();
    }

    //Recommender implementation from: https://github.com/apache/mahout/blob/trunk/website/users/recommender/userbased-5-minutes.md
    public List<String> getRecommendationsForUser(String user) throws IOException, TasteException {
        int numUser = this.usersIDStorage.get(user);
        List<String> recommendsList = new ArrayList<String>();
        DataModel model = new FileDataModel(new File("data/movies.csv"));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
        UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
        List<RecommendedItem> recommendations = recommender.recommend(numUser, 3);
        for (RecommendedItem recommendation : recommendations){
            recommendsList.add(getProductStringID((int)recommendation.getItemID()));
        }
        return recommendsList;
    }
}