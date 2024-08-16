# import psycopg2
# from django.core.management.base import BaseCommand
# from django.db import transaction
# from properties.models import Property, Location, Amenity, Image
# import json

# class Command(BaseCommand):
#     help = 'Migrate data from Scrapy PostgreSQL database to Django'

#     def handle(self, *args, **kwargs):
#         # Connect to the Scrapy PostgreSQL database
#         conn = psycopg2.connect(
#             dbname="scrap_hotel_data",
#             user="postgres",
#             password="p@stgress",
#             host="localhost",
#             port="5433"
#         )
#         cur = conn.cursor()

#         # Fetch all hotels from Scrapy database
#         cur.execute("SELECT * FROM hotels")
#         hotels = cur.fetchall()

#         with transaction.atomic():
#             for hotel in hotels:
#                 hotel_id, name, description, lat, lon, rating, amenities_str, images_str, address, city_name = hotel

#                 # Create or get the city location
#                 city, _ = Location.objects.get_or_create(
#                     name=city_name,
#                     type='city',
#                     defaults={'latitude': lat, 'longitude': lon}
#                 )

#                 # Create the property, using hotel_id as the primary key
#                 property, created = Property.objects.update_or_create(
#                     property_id=hotel_id,
#                     defaults={
#                         'title': name,
#                         'description': description
#                     }
#                 )
#                 property.locations.add(city)

#                 # Add amenities
#                 if amenities_str:
#                     amenities = json.loads(amenities_str)
#                     for amenity_name in amenities:
#                         amenity, _ = Amenity.objects.get_or_create(name=amenity_name)
#                         property.amenities.add(amenity)

#                 # Add images
#                 if images_str:
#                     images = images_str.split(',')
#                     for image_url in images:
#                         Image.objects.get_or_create(property=property, image=image_url)

#         cur.close()
#         conn.close()

#         self.stdout.write(self.style.SUCCESS('Successfully migrated data from Scrapy to Django'))


import psycopg2
from django.core.management.base import BaseCommand
from django.db import transaction
from properties.models import Property, Location, Amenity, Image
import json

class Command(BaseCommand):
    help = 'Migrate data from Scrapy PostgreSQL database to Django'

    def handle(self, *args, **kwargs):
        # Connect to the Scrapy PostgreSQL database
        conn = psycopg2.connect(
            dbname="scrap_hotel_data",
            user="postgres",
            password="p@stgress",
            host="localhost",
            port="5433"
        )
        cur = conn.cursor()

        # Fetch all hotels from Scrapy database
        cur.execute("SELECT * FROM hotels")
        hotels = cur.fetchall()

        with transaction.atomic():
            for hotel in hotels:
                hotel_id, name, description, lat, lon, rating, amenities_str, images_str, address, city_name = hotel

                # Create or get the city location
                city, _ = Location.objects.get_or_create(
                    name=city_name,
                    type='city',
                    defaults={'latitude': lat, 'longitude': lon}
                )

                # Create the property, using hotel_id as the primary key
                property, created = Property.objects.update_or_create(
                    property_id=hotel_id,
                    defaults={
                        'title': name,
                        'description': description
                    }
                )
                property.locations.add(city)

                # Add amenities
                if amenities_str:
                    # Split the amenities string into a list
                    amenities = [a.strip() for a in amenities_str.split(',') if a.strip()]
                    for amenity_name in amenities:
                        amenity, _ = Amenity.objects.get_or_create(name=amenity_name)
                        property.amenities.add(amenity)

                # Add images
                if images_str:
                    images = [img.strip() for img in images_str.split(',') if img.strip()]
                    for image_url in images:
                        Image.objects.get_or_create(property=property, image=image_url)

        cur.close()
        conn.close()

        self.stdout.write(self.style.SUCCESS('Successfully migrated data from Scrapy to Django'))