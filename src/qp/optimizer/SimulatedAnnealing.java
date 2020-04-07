package qp.optimizer;

import qp.operators.Debug;
import qp.operators.Operator;
import qp.utils.SQLQuery;

import java.util.Random;

public class SimulatedAnnealing extends RandomOptimizer {

    public static final double INITIAL_TEMPERATURE = 100;
    public static final double TEMPERATURE_THRESHOLD = 8;

    int numJoin;        // Number of joins in this query plan
    private int iteration;
    private Random random;

    public SimulatedAnnealing(SQLQuery sqlquery) {
        super(sqlquery);
        random = new Random();
        iteration = 0;
    }

    @Override
    public Operator getOptimizedPlan() {
        RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
        numJoin = rip.getNumJoins();
        double temperature = INITIAL_TEMPERATURE;
        Operator currPlan = rip.prepareInitialPlan();
        modifySchema(currPlan);
        System.out.println("-----------initial Plan-------------");
        Debug.PPrint(currPlan);
        PlanCost pc = new PlanCost();
        long initCost = pc.getCost(currPlan);
        System.out.println(initCost);

        while (true) {
            if (temperature < TEMPERATURE_THRESHOLD) {
                System.out.println("Cooled down! The result is obtained.");
                break;
            }

            Operator newPlan = getNeighbor((Operator) currPlan.clone());
            PlanCost oldPc = new PlanCost();
            PlanCost newPc = new PlanCost();
            long oldCost = oldPc.getCost(currPlan);
            long newCost = newPc.getCost(currPlan);
            if (isAccepted(temperature, newCost - oldCost)) {
                currPlan = newPlan;
            }

            temperature = scheduleNewTemperature();
            iteration++;
            System.out.println("Temperature: " + temperature);
            System.out.println("Cost: " + Math.min(oldCost, newCost));
        }

        return currPlan;
    }

    private boolean isAccepted(double temperature, double improvementFromOlderPlan) {
        double acceptanceProbability = getAcceptanceProbability(temperature, improvementFromOlderPlan);
        if (acceptanceProbability >= random.nextDouble()) {
            return true;
        }
        return false;
    }

    private double getAcceptanceProbability(double temperature, double improvementFromOlderPlan) {
        if (improvementFromOlderPlan > 0) {
            return 1.0;
        } else {
            return Math.exp((improvementFromOlderPlan) / temperature);
        }
    }

    private double scheduleNewTemperature() {
        double newTemperature = INITIAL_TEMPERATURE / (1 + Math.log(1 + iteration));
        return newTemperature;
    }

}
