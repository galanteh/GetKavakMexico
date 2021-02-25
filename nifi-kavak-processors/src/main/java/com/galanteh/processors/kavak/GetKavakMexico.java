/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.galanteh.processors.kavak;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.components.*;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;

@SupportsBatching
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"kavak", "web scrapping", "used cars", "mexico"})
@CapabilityDescription("Processor that run a web scrapping over Kavak.com to extract all the used cars listed.")
@SeeAlso({})
@WritesAttributes({@WritesAttribute(attribute = "mime.type", description = "Sets mime type to application/json")})

public class GetKavakMexico extends AbstractProcessor {

    final static Integer CONST_YEAR_MIN = 2010;
    final static Integer CONST_YEAR_MAX = LocalDateTime.now().getYear() + 1;
    final static Integer CONST_ODOMETER_MAX = 300000;
    BlockingQueue queue = new ArrayBlockingQueue(10000);
    ZonedDateTime last_harvest = null;
    boolean verbose = true;

    static final AllowableValue ALL_CARS = new AllowableValue("ALL_CARS", "All the Cars", "All the Cars");
    static final AllowableValue ACURA = new AllowableValue("ACURA", "ACURA", "Only ACURA Cars");
    static final AllowableValue ALFA_ROMEO = new AllowableValue("ALFA_ROMEO", "ALFA ROMEO", "Only ALFA ROMEO Cars");
    static final AllowableValue AUDI = new AllowableValue("AUDI", "AUDI", "Only AUDI Cars");
    static final AllowableValue BUICK = new AllowableValue("BUICK", "BUICK", "Only BUICK Cars");
    static final AllowableValue BMW = new AllowableValue("BMW", "BMW", "Only BMW Cars");
    static final AllowableValue CADILLAC = new AllowableValue("CADILLAC", "CADILLAC", "Only CADILLAC Cars");
    static final AllowableValue CHEVROLET = new AllowableValue("CHEVROLET", "CHEVROLET", "Only CHEVROLET Cars");
    static final AllowableValue CHRYSLER = new AllowableValue("CHRYSLER", "CHRYSLER", "Only CHRYSLER Cars");
    static final AllowableValue DODGE = new AllowableValue("DODGE", "DODGE", "Only DODGE Cars");
    static final AllowableValue FIAT = new AllowableValue("FIAT", "FIAT", "Only FIAT Cars");
    static final AllowableValue FORD = new AllowableValue("FORD", "FORD", "Only FORD Cars");
    static final AllowableValue GMC = new AllowableValue("GMC", "GMC", "Only GMC Cars");
    static final AllowableValue HONDA = new AllowableValue("HONDA", "HONDA", "Only HONDA Cars");
    static final AllowableValue HYUNDAI = new AllowableValue("HYUNDAI", "HYUNDAI", "Only HYUNDAI Cars");
    static final AllowableValue INFINITI = new AllowableValue("INFINITI", "INFINITI", "Only INFINITI Cars");
    static final AllowableValue JAGUAR = new AllowableValue("JAGUAR", "JAGUAR", "Only JAGUAR Cars");
    static final AllowableValue JEEP = new AllowableValue("JEEP", "JEEP", "Only JEEP Cars");
    static final AllowableValue KIA = new AllowableValue("KIA", "KIA", "Only KIA Cars");
    static final AllowableValue LAND_ROVER = new AllowableValue("LAND_ROVER", "LAND ROVER", "Only LAND ROVER Cars");
    static final AllowableValue LINCOLN = new AllowableValue("LINCOLN", "LINCOLN", "Only LINCOLN Cars");
    static final AllowableValue MAZDA = new AllowableValue("MAZDA", "MAZDA", "Only MAZDA Cars");
    static final AllowableValue MERCEDEZ_BENZ = new AllowableValue("MERCEDEZ_BENZ", "MERCEDEZ BENZ", "Only MERCEDEZ BENZ Cars");
    static final AllowableValue MINI = new AllowableValue("MINI", "MINI", "Only MINI Cars");
    static final AllowableValue MITSUBISHI = new AllowableValue("MITSUBISHI", "MITSUBISHI", "Only MITSUBISHI Cars");
    static final AllowableValue NISSAN = new AllowableValue("NISSAN", "NISSAN", "Only NISSAN Cars");
    static final AllowableValue PEUGEOT = new AllowableValue("PEUGEOT", "PEUGEOT", "Only PEUGEOT Cars");
    static final AllowableValue PORSCHE = new AllowableValue("PORSCHE", "PORSCHE", "Only PORSCHE Cars");
    static final AllowableValue RENAULT = new AllowableValue("RENAULT", "RENAULT", "Only RENAULT Cars");
    static final AllowableValue SEAT = new AllowableValue("SEAT", "SEAT", "Only SEAT Cars");
    static final AllowableValue SMART = new AllowableValue("SMART", "SMART", "Only SMART Cars");
    static final AllowableValue SUBARU = new AllowableValue("SUBARU", "SUBARU", "Only SUBARU Cars");
    static final AllowableValue SUZUKI = new AllowableValue("SUZUKI", "SUZUKI", "Only SUZUKI Cars");
    static final AllowableValue TOYOTA = new AllowableValue("TOYOTA", "TOYOTA", "Only TOYOTA Cars");
    static final AllowableValue VOLKSWAGEN = new AllowableValue("VOLKSWAGEN", "VOLKSWAGEN", "Only VOLKSWAGEN Cars");
    static final AllowableValue VOLVO = new AllowableValue("VOLVO", "VOLVO", "Only VOLVO Cars");

    public static final PropertyDescriptor MANUFACTURER = new PropertyDescriptor
            .Builder().name("MANUFACTURER")
            .displayName("Manufacturer")
            .defaultValue(ALL_CARS.getValue())
            .description("Manufacturer of the car.")
            .required(true)
            .allowableValues(ALL_CARS, ACURA, ALFA_ROMEO, AUDI, BUICK, BMW, CADILLAC, CHEVROLET, CHRYSLER, DODGE, FIAT, FORD, GMC, HONDA, HYUNDAI, INFINITI, JAGUAR, JEEP, KIA, LAND_ROVER, LINCOLN, MAZDA, MERCEDEZ_BENZ, MINI, MITSUBISHI, NISSAN, PEUGEOT, PORSCHE, RENAULT, SEAT, SMART, SUBARU, SUZUKI, TOYOTA, VOLKSWAGEN, VOLVO)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor YEAR_MIN = new PropertyDescriptor
            .Builder().name("YEAR_MIN")
            .displayName("Minimum Year")
            .defaultValue(CONST_YEAR_MIN.toString())
            .description("Minimum Year of the car.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor YEAR_MAX = new PropertyDescriptor
            .Builder().name("YEAR_MAX")
            .displayName("Maximum Year")
            .defaultValue(CONST_YEAR_MAX.toString())
            .description("Maximum Year of the car.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor ODOMETER_MIN = new PropertyDescriptor
            .Builder().name("ODOMETER_MIN")
            .displayName("Minimum Kilometers")
            .defaultValue("1")
            .description("Minimum Kilometers of the car.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor ODOMETER_MAX = new PropertyDescriptor
            .Builder().name("ODOMETER_MAX")
            .displayName("Maximum Kilometers")
            .defaultValue(CONST_ODOMETER_MAX.toString())
            .description("Maximum Kilometers of the car.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MINIMUM_TIME_BETWEEN_SCRAPPING = new PropertyDescriptor
            .Builder().name("MINIMUM_TIME_BETWEEN_SCRAPPING")
            .displayName("Minimum time between scrapping in minutes. Once weekly by default")
            .defaultValue("10080")
            .description("Minimum time between scrapping in minutes. Once weekly by default")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor VERBOSE = new PropertyDescriptor
            .Builder().name("VERBOSE")
            .displayName("Verbose")
            .defaultValue("true")
            .description("Verbose mode.")
            .required(true)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("cars")
            .description("All car information will be routed to this relationship")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MANUFACTURER);
        descriptors.add(YEAR_MIN);
        descriptors.add(YEAR_MAX);
        descriptors.add(ODOMETER_MIN);
        descriptors.add(ODOMETER_MAX);
        descriptors.add(MINIMUM_TIME_BETWEEN_SCRAPPING);
        descriptors.add(VERBOSE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    private void set_harvest_time(){
        ZonedDateTime ldt = LocalDateTime.now().atZone(this.get_timezone());
        this.last_harvest = ldt;
    }

    private ZoneId get_timezone()
    {
        return TimeZone.getTimeZone("America/Mexico_City").toZoneId();
    }

    private boolean should_run_scrapping(Integer minimum_time_to_harvest){
        if (this.last_harvest == null) { return true; }
        ZonedDateTime now = LocalDateTime.now().atZone(this.get_timezone()).minusMinutes(60);
        long diff = ChronoUnit.MINUTES.between(now, this.last_harvest);
        return !(diff > 0 && diff <= minimum_time_to_harvest);
    }

    private void run_scrapping(ProcessContext context) {
        final String manufacturer_filter = context.getProperty(MANUFACTURER).getValue();
        final Integer year_min_filter = Integer.parseInt(context.getProperty(YEAR_MIN).getValue());
        final Integer year_max_filter = Integer.parseInt(context.getProperty(YEAR_MAX).getValue());
        final Integer odometer_min_filter = Integer.parseInt(context.getProperty(ODOMETER_MIN).getValue());
        final Integer odometer_max_filter = Integer.parseInt(context.getProperty(ODOMETER_MAX).getValue());
        this.verbose = Boolean.parseBoolean(context.getProperty(VERBOSE).getValue());
        new Thread(new KavakScrappingWebsite(verbose, this.getLogger(), this.queue, manufacturer_filter, year_min_filter, year_max_filter, odometer_min_filter, odometer_max_filter)).start();
        this.set_harvest_time();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();

        // YEAR's Validations
        final Integer year_min = Integer.parseInt(validationContext.getProperty(YEAR_MIN).getValue());
        final Integer year_max = Integer.parseInt(validationContext.getProperty(YEAR_MAX).getValue());
        final String YEAR_MIN_displayName = YEAR_MIN.getDisplayName();
        final String YEAR_MAX_displayName = YEAR_MAX.getDisplayName();

        // YEAR MIN BELOW MIN
        if (year_min < CONST_YEAR_MIN) {
            results.add(new ValidationResult.Builder()
                    .subject(YEAR_MIN_displayName)
                    .explanation(String.format("'%s' is required to be greater than year %d.", YEAR_MIN_displayName, CONST_YEAR_MIN))
                    .valid(false)
                    .build());
        }

        // YEAR MAX more than year now plus one year.
        if (year_max > CONST_YEAR_MAX) {
            results.add(new ValidationResult.Builder()
                    .subject(YEAR_MAX_displayName)
                    .explanation(String.format("'%s' is required to be lower or equal to year %s.", YEAR_MAX_displayName, CONST_YEAR_MAX))
                    .valid(false)
                    .build());
        }

        // YEAR MIN > YEAR MAX
        if (year_min > year_max) {
            results.add(new ValidationResult.Builder()
                    .subject(YEAR_MIN_displayName)
                    .explanation(String.format("%s can not be greater than %s", YEAR_MIN_displayName, YEAR_MAX_displayName))
                    .valid(false)
                    .build());
        }

        // ODOMETER's Validations
        final Integer odometer_min = Integer.parseInt(validationContext.getProperty(ODOMETER_MIN).getValue());
        final Integer odometer_max = Integer.parseInt(validationContext.getProperty(ODOMETER_MAX).getValue());
        final String ODOMETER_MIN_displayName = ODOMETER_MIN.getDisplayName();
        final String ODOMETER_MAX_displayName = ODOMETER_MAX.getDisplayName();

        // ODOMETER MAX not more than 300K KM.
        if (year_max > CONST_ODOMETER_MAX) {
            results.add(new ValidationResult.Builder()
                    .subject(YEAR_MAX_displayName)
                    .explanation(String.format("'%s' is required to be lower or equal to year %s.", YEAR_MAX_displayName, CONST_YEAR_MAX))
                    .valid(false)
                    .build());
        }

        // ODOMETER MIN > ODOMETER MAX
        if (odometer_min > odometer_max) {
            results.add(new ValidationResult.Builder()
                    .subject(ODOMETER_MIN_displayName)
                    .explanation(String.format("%s can not be greater than %s", ODOMETER_MIN_displayName, ODOMETER_MAX_displayName))
                    .valid(false)
                    .build());
        }

        return results;
    };


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        Integer minimum_time_to_harvest = Integer.parseInt(context.getProperty(MINIMUM_TIME_BETWEEN_SCRAPPING).getValue());

        final Car car = (Car) this.queue.poll();

        if (car == null)
        {
            if (this.should_run_scrapping(minimum_time_to_harvest)) {
                if (verbose) {  this.getLogger().info("Starting Scrapping ... "); }
                this.run_scrapping(context);
            }
        }
        else {
            if (this.should_run_scrapping(minimum_time_to_harvest)) {
                if (verbose) { this.getLogger().info("Scrapping will not run. Minimum time not meet yet."); }
                context.yield();
                return;
            }
        }

        if (car != null) {
            FlowFile flowFile = session.create();
            if (flowFile == null) {
                context.yield();
                return; };
            if (verbose) {
                this.getLogger().info("Setting the attributes at the FlowFile.");
            }
            final Map<String, String> attributes = new HashMap<>();
            attributes.put("URL", car.url);
            attributes.put("MANUFACTURER", car.manufacturer);
            attributes.put("MODEL", car.model);
            attributes.put("YEAR", car.year.toString());
            attributes.put("ODOMETER", car.odometer.toString());
            attributes.put("LOCATION", car.location);
            attributes.put("TRANSMISSION", car.transmission);
            attributes.put("FUEL", car.fuel);
            attributes.put("CONDITION", car.condition);
            attributes.put("DETAIL DOTS", car.details_dots.toString());
            attributes.put("PRICE", car.price.toString());

            try {
                flowFile = session.putAllAttributes(flowFile, attributes);
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        out.write(car.getBytes(StandardCharsets.UTF_8));
                    }
                });
                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().receive(flowFile, car.url);

            } catch (ProcessException ex) {
                getLogger().error(String.format("%s", ex.getMessage()));
                return;
            }
        }
    }
}
